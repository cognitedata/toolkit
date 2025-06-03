import csv
import importlib.util
import json
from abc import abstractmethod
from dataclasses import dataclass
from functools import lru_cache
from io import TextIOWrapper
from pathlib import Path
from types import MappingProxyType
from typing import IO, TYPE_CHECKING, Any, ClassVar, Generic, Literal, TypeAlias, TypeVar

from cognite_toolkit._cdf_tk.exceptions import ToolkitMissingDependencyError, ToolkitValueError
from cognite_toolkit._cdf_tk.utils import humanize_collection, to_directory_compatible
from cognite_toolkit._cdf_tk.utils.file import yaml_safe_dump

if TYPE_CHECKING:
    import pyarrow as pa
    import pyarrow.parquet as pq

FileFormat: TypeAlias = Literal["csv", "parquet", "yaml"]
DataType: TypeAlias = Literal["string", "integer", "float", "boolean", "json"]
JsonVal: TypeAlias = None | str | int | float | bool | dict[str, "JsonVal"] | list["JsonVal"]
Rows: TypeAlias = list[dict[str, str | int | float | bool | JsonVal | None]]


@dataclass
class SchemaColumn:
    name: str
    type: DataType
    is_array: bool = False

    def __post_init__(self) -> None:
        if self.type == "json" and self.is_array:
            raise ValueError("JSON columns cannot be arrays. Use 'is_array=False' for JSON columns.")


@dataclass
class Schema:
    display_name: str
    folder_name: str
    kind: str
    format_: FileFormat
    columns: list[SchemaColumn]


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

        table = pa.Table.from_pylist(rows, schema=self._create_schema())
        writer.write_table(table)

    def _is_above_file_size_limit(self, filepath: Path, writer: "pq.ParquetWriter") -> bool:
        return filepath.exists() and filepath.stat().st_size > self.max_file_size_bytes

    @lru_cache(maxsize=1)
    def _json_columns(self) -> set[str]:
        """Check if the writer supports JSON format."""
        return {col.name for col in self.schema.columns if col.type == "json"}

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
        elif type_ == "datetime":
            pa_type = pa.timestamp("ms")
        elif type_ == "date":
            pa_type = pa.date32()
        elif type_ == "time":
            pa_type = pa.time64("ms")
        elif type_ == "json":
            pa_type = pa.string()
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
