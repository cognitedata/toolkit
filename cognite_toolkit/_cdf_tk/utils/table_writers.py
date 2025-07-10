import csv
import importlib.util
import json
import sys
from abc import abstractmethod
from collections.abc import Collection, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from functools import lru_cache
from io import TextIOWrapper
from pathlib import Path
from types import MappingProxyType
from typing import IO, TYPE_CHECKING, Any, ClassVar, Generic, Literal, SupportsIndex, TypeAlias, TypeVar, overload

from cognite.client.data_classes.data_modeling import data_types as dt
from cognite.client.data_classes.data_modeling.views import MappedProperty, ViewProperty

from cognite_toolkit._cdf_tk.exceptions import ToolkitMissingDependencyError, ToolkitTypeError, ToolkitValueError
from cognite_toolkit._cdf_tk.utils import humanize_collection, to_directory_compatible
from cognite_toolkit._cdf_tk.utils.file import yaml_safe_dump

from .useful_types import JsonVal

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

if TYPE_CHECKING:
    import pyarrow as pa
    import pyarrow.parquet as pq

FileFormat: TypeAlias = Literal["csv", "parquet", "yaml"]
DataType: TypeAlias = Literal["string", "integer", "float", "boolean", "json", "date", "timestamp"]
PrimaryCellValue: TypeAlias = datetime | date | str | int | float | bool | JsonVal | None
CellValue: TypeAlias = PrimaryCellValue | list[PrimaryCellValue]
Rows: TypeAlias = list[dict[str, CellValue]]


@dataclass(frozen=True)
class SchemaColumn:
    name: str
    type: DataType
    is_array: bool = False

    def __post_init__(self) -> None:
        if self.type == "json" and self.is_array:
            raise ValueError("JSON columns cannot be arrays. Use 'is_array=False' for JSON columns.")


class SchemaColumnList(list, Sequence[SchemaColumn]):
    # Implemented to get correct type hints
    def __init__(self, collection: Collection[SchemaColumn] | None = None) -> None:
        super().__init__(collection or [])

    def __iter__(self) -> Iterator[SchemaColumn]:
        return super().__iter__()

    @overload
    def __getitem__(self, index: SupportsIndex) -> SchemaColumn: ...

    @overload
    def __getitem__(self, index: slice) -> Self: ...

    def __getitem__(self, index: SupportsIndex | slice, /) -> SchemaColumn | Self:
        if isinstance(index, slice):
            return type(self)(super().__getitem__(index))
        return super().__getitem__(index)

    @classmethod
    def create_from_view_properties(cls, properties: Mapping[str, ViewProperty], support_edges: bool = False) -> Self:
        """Create a SchemaColumnList from a mapping of ViewProperty objects.

        Args:
            properties (Mapping[str, ViewProperty]): A mapping of property names to ViewProperty objects.
            support_edges (bool): Whether the the view supports edges. If True, the schema will include
                startNode and endNode columns.

        Returns:
            SchemaColumnList: A list of SchemaColumn objects representing the properties.
        """
        columns = [
            SchemaColumn("space", "string", is_array=False),
            SchemaColumn("externalId", "string", is_array=False),
            SchemaColumn("instanceType", "string"),
            SchemaColumn("existingVersion", "integer", is_array=False),
            SchemaColumn("type", "json", is_array=False),
        ]
        if support_edges:
            columns.append(SchemaColumn("startNode", "json", is_array=False))
            columns.append(SchemaColumn("endNode", "json", is_array=False))
        for name, prop in properties.items():
            if not isinstance(prop, MappedProperty):
                # We skip all properties that does not reside in a container.
                continue
            schema_type = cls._dms_to_schema_type(prop.type)
            is_array = (
                isinstance(prop.type, dt.ListablePropertyType)
                and prop.type.is_list
                and schema_type != "json"  # JSON is not an array type
            )
            columns.append(SchemaColumn(name=f"properties.{name}", type=schema_type, is_array=is_array))
        return cls(columns)

    @classmethod
    def _dms_to_schema_type(cls, model_type: dt.PropertyType) -> DataType:
        if isinstance(model_type, dt.Text | dt.Enum | dt.CDFExternalIdReference):
            return "string"
        elif isinstance(model_type, dt.Boolean):
            return "boolean"
        elif isinstance(model_type, dt.Json | dt.DirectRelation):
            return "json"
        elif isinstance(model_type, dt.Int32 | dt.Int64):
            return "integer"
        elif isinstance(model_type, dt.Float32 | dt.Float64):
            return "float"
        elif isinstance(model_type, dt.Timestamp):
            return "timestamp"
        elif isinstance(model_type, dt.Date):
            return "date"
        else:
            raise ToolkitTypeError(
                f"Failed convertion from data modeling type to Table Schema. Unknown type: {type(model_type)!r}."
            )


@dataclass
class Schema:
    display_name: str
    folder_name: str
    kind: str
    format_: FileFormat
    columns: SchemaColumnList


T_IO = TypeVar("T_IO", bound=IO)


class TableFileWriter(Generic[T_IO]):
    encoding = "utf-8"
    newline = "\n"
    format: ClassVar[FileFormat]

    def __init__(self, schema: Schema, output_dir: Path, max_file_size_bytes: int = 128 * 1024 * 1024) -> None:
        self.max_file_size_bytes = max_file_size_bytes
        self.schema = schema
        self.output_dir = output_dir
        self._file_count = 1
        self._writer_by_filepath: dict[Path, T_IO] = {}

    def write_rows(self, rows_group_list: list[tuple[str, Rows]]) -> None:
        """Write rows to a file."""
        for group, group_rows in rows_group_list:
            if not group_rows:
                continue
            writer = self._get_writer(group)
            self._write_rows(writer, group_rows)

    @abstractmethod
    def _write_rows(self, writer: T_IO, rows: Rows) -> None:
        raise NotImplementedError()

    @abstractmethod
    def _create_writer(self, filepath: Path) -> T_IO:
        """Create a writer for the given file path."""
        raise NotImplementedError("This method should be implemented in subclasses.")

    @abstractmethod
    def _is_above_file_size_limit(self, filepath: Path, writer: T_IO) -> bool:
        """Check if the file size is above the limit."""
        raise NotImplementedError("This method should be implemented in subclasses.")

    def __enter__(self) -> "TableFileWriter":
        self._file_count = 1
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any | None) -> None:
        for writer in self._writer_by_filepath.values():
            writer.close()
        self._writer_by_filepath.clear()
        return None

    def _get_writer(self, group: str) -> T_IO:
        clean_name = f"{to_directory_compatible(group)}-" if group else ""
        file_path = (
            self.output_dir
            / self.schema.folder_name
            / f"{clean_name}part-{self._file_count:04}.{self.schema.kind}.{self.format}"
        )
        file_path.parent.mkdir(parents=True, exist_ok=True)
        if file_path not in self._writer_by_filepath:
            self._writer_by_filepath[file_path] = self._create_writer(file_path)
        elif self._is_above_file_size_limit(file_path, self._writer_by_filepath[file_path]):
            self._writer_by_filepath[file_path].close()
            del self._writer_by_filepath[file_path]
            self._file_count += 1
            return self._get_writer(group)
        return self._writer_by_filepath[file_path]

    @classmethod
    def get_write_cls(cls, format_: FileFormat) -> "type[TableFileWriter]":
        """Get the writer class for the given format."""
        write_cls = _TABLEWRITER_CLASS_BY_FORMAT.get(format_)
        if write_cls is None:
            raise ToolkitValueError(
                f"Unsupported format {format_}. Supported formats are {humanize_collection(_TABLEWRITER_CLASS_BY_FORMAT.keys())}."
            )
        return write_cls


class ParquetWriter(TableFileWriter["pq.ParquetWriter"]):
    """Parquet writer for CDF Toolkit.

    Caveat: This mutates the rows to convert JSON, timestamp, and date columns to appropriate formats.
    This is necessary because pyarrow does not support JSON, timestamp, and date types directly in the way we need.
    We avoid making a copy of each row for performance reasons, but this means that the rows passed to this writer
    will be modified in place.
    """

    format = "parquet"

    def __init__(self, schema: Schema, output_dir: Path, max_file_size_bytes: int = 128 * 1024 * 1024) -> None:
        super().__init__(schema, output_dir, max_file_size_bytes)
        self._check_pyarrow_dependency()

    def _create_writer(self, filepath: Path) -> "pq.ParquetWriter":
        import pyarrow.parquet as pq

        schema = self._create_schema()
        return pq.ParquetWriter(filepath, schema)

    def _write_rows(self, writer: "pq.ParquetWriter", rows: Rows) -> None:
        import pyarrow as pa

        if json_columns := self._json_columns():
            for row in rows:
                json_values = set(row.keys()) & json_columns
                for col in json_values:
                    row[col] = json.dumps(row[col])
        if timestamp_columns := self._timestamp_columns():
            for row in rows:
                for col in set(row.keys()) & timestamp_columns:
                    cell_value = row[col]
                    if isinstance(cell_value, list):
                        # MyPy does not understand that a list of PrimaryCellValue is valid here
                        # It expects a union of PrimaryCellValue and list[PrimaryCellValue].
                        row[col] = [self._to_datetime(value) for value in cell_value]  # type: ignore[assignment]
                    else:
                        row[col] = self._to_datetime(cell_value)
        if date_columns := self._date_columns():
            for row in rows:
                for col in set(row.keys()) & date_columns:
                    cell_value = row[col]
                    if isinstance(cell_value, list):
                        # MyPy does not understand that a list of PrimaryCellValue is valid here.
                        # It expects a union of PrimaryCellValue and list[PrimaryCellValue].
                        row[col] = [self._to_date(value) for value in cell_value]  # type: ignore[assignment]
                    else:
                        row[col] = self._to_date(cell_value)

        table = pa.Table.from_pylist(rows, schema=self._create_schema())
        writer.write_table(table)

    def _is_above_file_size_limit(self, filepath: Path, writer: "pq.ParquetWriter") -> bool:
        return filepath.exists() and filepath.stat().st_size > self.max_file_size_bytes

    @lru_cache(maxsize=1)
    def _json_columns(self) -> set[str]:
        """Check if the writer supports JSON format."""
        return {col.name for col in self.schema.columns if col.type == "json"}

    @lru_cache(maxsize=1)
    def _timestamp_columns(self) -> set[str]:
        """Check if the writer supports timestamp format."""
        return {col.name for col in self.schema.columns if col.type == "timestamp"}

    @lru_cache(maxsize=1)
    def _date_columns(self) -> set[str]:
        return {col.name for col in self.schema.columns if col.type == "date"}

    @classmethod
    def _to_datetime(cls, value: CellValue) -> CellValue:
        if isinstance(value, datetime) or value is None:
            output = value
        elif isinstance(value, date):
            output = datetime.combine(value, datetime.min.time())
        elif isinstance(value, int | float):
            # Assuming the value is a timestamp in milliseconds
            output = datetime.fromtimestamp(value / 1000.0)
        elif isinstance(value, str):
            output = cls._convert_data_modelling_timestamp(value)
        else:
            raise ToolkitTypeError(
                f"Unsupported value type for datetime conversion: {type(value)}. Expected datetime, date, int, float, or str."
            )
        if output is not None and output.tzinfo is None:
            # Ensure the datetime is in UTC
            output = output.replace(tzinfo=timezone.utc)
        elif output is not None and output.tzinfo is not None:
            # Convert to UTC if it has a timezone
            output = output.astimezone(timezone.utc)
        return output

    @classmethod
    def _to_date(cls, value: CellValue) -> CellValue:
        if isinstance(value, date) or value is None:
            return value
        elif isinstance(value, datetime):
            return value.date()
        elif isinstance(value, int | float):
            # Assuming the value is a timestamp in milliseconds
            return date.fromtimestamp(value / 1000.0)
        elif isinstance(value, str):
            return cls._convert_data_modelling_timestamp(value).date()
        else:
            raise ToolkitTypeError(
                f"Unsupported value type for date conversion: {type(value)}. Expected date, datetime, int, float, or str."
            )

    @classmethod
    def _convert_data_modelling_timestamp(cls, timestamp: str) -> datetime:
        """Convert a timestamp string from the data modeling format to a datetime object."""
        try:
            return datetime.fromisoformat(timestamp)
        except ValueError:
            # Typically hits if the timestamp has truncated milliseconds,
            # For example, "2021-01-01T00:00:00.17+00:00".
            # In Python 3.10, the strptime requires exact formats so we need both formats below.
            # In Python 3.11-13, if the timestamp matches on the second it will match on the first,
            # so when we set lower bound to 3.11 the loop will not be needed.
            for format_ in ["%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"]:
                try:
                    return datetime.strptime(timestamp, format_)
                except ValueError:
                    continue
            raise ValueError(
                f"Invalid timestamp format: {timestamp}. Expected ISO 8601 format with optional milliseconds and timezone."
            )

    @lru_cache(maxsize=1)
    def _create_schema(self) -> "pa.Schema":
        """Create a pyarrow schema from the schema definition."""
        self._check_pyarrow_dependency()
        import pyarrow as pa

        fields: list[pa.Field] = []
        for prop in self.schema.columns:
            pa_type = self._as_pa_type(prop.type, prop.is_array)
            fields.append(pa.field(prop.name, pa_type, nullable=True))
        return pa.schema(fields)

    @staticmethod
    def _check_pyarrow_dependency() -> None:
        if importlib.util.find_spec("pyarrow") is None:
            raise ToolkitMissingDependencyError(
                "Writing to parquet requires pyarrow. Install with 'pip install \"cognite-toolkit[table]\"'"
            )

    @staticmethod
    def _as_pa_type(type_: DataType, is_array: bool) -> "pa.DataType":
        """Convert a data type to a pyarrow type."""
        import pyarrow as pa

        if type_ == "string":
            pa_type = pa.string()
        elif type_ == "integer":
            pa_type = pa.int64()
        elif type_ == "float":
            pa_type = pa.float64()
        elif type_ == "boolean":
            pa_type = pa.bool_()
        elif type_ == "date":
            pa_type = pa.date32()
        elif type_ == "time":
            pa_type = pa.time64("ms")
        elif type_ == "json":
            pa_type = pa.string()
        elif type_ == "timestamp":
            pa_type = pa.timestamp("ms", tz="UTC")
        else:
            raise ToolkitValueError(f"Unsupported data type {type_}.")

        if is_array:
            pa_type = pa.list_(pa_type)
        return pa_type


class CSVWriter(TableFileWriter[TextIOWrapper]):
    format = "csv"

    def _create_writer(self, filepath: Path) -> TextIOWrapper:
        stream = filepath.open("a", encoding=self.encoding, newline=self.newline)
        writer = self._create_dict_writer(stream)
        if filepath.stat().st_size == 0:
            writer.writeheader()
        return stream

    def _is_above_file_size_limit(self, filepath: Path, writer: TextIOWrapper) -> bool:
        current_position = writer.tell()
        writer.seek(0, 2)
        if writer.tell() > self.max_file_size_bytes:
            return True
        writer.seek(current_position)
        return False

    def _write_rows(self, writer: TextIOWrapper, rows: Rows) -> None:
        dict_writer = self._create_dict_writer(writer)
        dict_writer.writerows(rows)

    def _create_dict_writer(self, writer: TextIOWrapper) -> csv.DictWriter:
        return csv.DictWriter(writer, fieldnames=[col.name for col in self.schema.columns], extrasaction="ignore")


class YAMLWriter(TableFileWriter[TextIOWrapper]):
    format = "yaml"

    def _create_writer(self, filepath: Path) -> TextIOWrapper:
        return filepath.open("a", encoding=self.encoding, newline=self.newline)

    def _is_above_file_size_limit(self, filepath: Path, writer: TextIOWrapper) -> bool:
        current_position = writer.tell()
        writer.seek(0, 2)
        if writer.tell() > self.max_file_size_bytes:
            return True
        writer.seek(current_position)
        return False

    def _write_rows(self, writer: TextIOWrapper, rows: Rows) -> None:
        writer.write(yaml_safe_dump(rows))


_TABLEWRITER_CLASS_BY_FORMAT: MappingProxyType[str, type[TableFileWriter]] = MappingProxyType(
    {w.format: w for w in TableFileWriter.__subclasses__()}  # type: ignore[type-abstract]
)
