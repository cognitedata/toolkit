import csv
import importlib.util
from abc import abstractmethod
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, ClassVar, Literal, TypeAlias

from cognite_toolkit._cdf_tk.exceptions import ToolkitMissingDependencyError, ToolkitValueError
from cognite_toolkit._cdf_tk.utils import humanize_collection, to_directory_compatible
from cognite_toolkit._cdf_tk.utils.file import yaml_safe_dump

if TYPE_CHECKING:
    import pyarrow as pa

FileFormat: TypeAlias = Literal["csv", "parquet", "yaml"]
DataType: TypeAlias = Literal["string", "integer", "float", "boolean", "datetime", "date", "time", "json"]
Rows: TypeAlias = list[dict[str, Any]]


@dataclass
class SchemaColumn:
    name: str
    type: DataType


@dataclass
class Schema:
    display_name: str
    folder_name: str
    kind: str
    format_: FileFormat
    columns: list[SchemaColumn]


class TableFileWriter:
    # 128 MB
    file_size = 128 * 1024 * 1024
    encoding = "utf-8"
    newline = "\n"
    format: ClassVar[FileFormat]

    def __init__(self, schema: Schema, output_dir: Path) -> None:
        self.schema = schema
        self.output_dir = output_dir

    @abstractmethod
    def write_rows(self, rows_group_list: list[tuple[str, Rows]]) -> None:
        """Write rows to a file."""
        raise NotImplementedError("This method should be implemented in subclasses.")

    @classmethod
    def load(cls, schema: Schema, output_directory: Path) -> "TableFileWriter":
        write_cls = _TABLEWRITER_CLASS_BY_FORMAT.get(schema.format_)
        if write_cls is None:
            raise ToolkitValueError(
                f"Unsupported format {schema.format_}. Supported formats are {humanize_collection(_TABLEWRITER_CLASS_BY_FORMAT.keys())}."
            )

        return write_cls(schema, output_directory)


class ParquetWrite(TableFileWriter):
    format = "parquet"

    def __init__(self, schema: Schema, output_dir: Path) -> None:
        super().__init__(schema, output_dir)
        self._check_pyarrow_dependency()

    def write_rows(self, rows_group_list: list[tuple[str, Rows]]) -> None:
        schema = self._create_schema()

        import pyarrow as pa
        import pyarrow.parquet as pq

        for group, group_rows in rows_group_list:
            if not group_rows:
                continue
            clean_name = to_directory_compatible(group) if group else "my"
            file_path = self.output_dir / self.schema.folder_name / f"{clean_name}.{self.schema.kind}.parquet"
            file_path.parent.mkdir(parents=True, exist_ok=True)
            table = pa.Table.from_pylist(group_rows, schema=schema)
            pq.write_table(table, file_path, row_group_size=self.file_size)

    def _create_schema(self) -> "pa.Schema":
        """Create a pyarrow schema from the schema definition."""
        self._check_pyarrow_dependency()
        import pyarrow as pa

        fields: list[pa.Field] = []
        for prop in self.schema.columns:
            pa_type = self._as_pa_type(prop.type)
            fields.append(pa.field(prop.name, pa_type, nullable=True))
        return pa.schema(fields)

    @staticmethod
    def _check_pyarrow_dependency() -> None:
        if importlib.util.find_spec("pyarrow") is None:
            raise ToolkitMissingDependencyError(
                "Writing to parquet requires pyarrow. Install with 'pip install cognite-toolkit[table]'"
            )

    @staticmethod
    def _as_pa_type(type_: DataType) -> "pa.DataType":
        """Convert a data type to a pyarrow type."""
        import pyarrow as pa

        if type_ == "string":
            return pa.string()
        elif type_ == "integer":
            return pa.int64()
        elif type_ == "float":
            return pa.float64()
        elif type_ == "boolean":
            return pa.bool_()
        elif type_ == "datetime":
            return pa.timestamp("ms")
        elif type_ == "date":
            return pa.date32()
        elif type_ == "time":
            return pa.time64("ms")
        elif type_ == "json":
            return pa.string()
        else:
            raise ToolkitValueError(f"Unsupported data type {type_}.")


class CSVWriter(TableFileWriter):
    format = "csv"

    def write_rows(self, rows_group_list: list[tuple[str, Rows]]) -> None:
        for group, group_rows in rows_group_list:
            if not group_rows:
                continue
            clean_name = to_directory_compatible(group) if group else "my"
            file_path = self.output_dir / self.schema.folder_name / f"{clean_name}.{self.schema.kind}.csv"
            file_path.parent.mkdir(parents=True, exist_ok=True)
            with file_path.open("a", encoding=self.encoding, newline=self.newline) as f:
                writer = csv.DictWriter(f, fieldnames=[col.name for col in self.schema.columns], extrasaction="ignore")
                if file_path.stat().st_size == 0:
                    writer.writeheader()
                writer.writerows(group_rows)


class YAMLWriter(TableFileWriter):
    format = "yaml"

    def write_rows(self, rows_group_list: list[tuple[str, Rows]]) -> None:
        for group, group_rows in rows_group_list:
            if not group_rows:
                continue
            clean_name = to_directory_compatible(group) if group else "my"
            file_path = self.output_dir / self.schema.folder_name / f"{clean_name}.{self.schema.kind}.yaml"
            file_path.parent.mkdir(parents=True, exist_ok=True)
            if file_path.exists():
                with file_path.open("a", encoding=self.encoding, newline=self.newline) as f:
                    f.write("\n")
                    f.write(yaml_safe_dump(group_rows))
            else:
                with file_path.open("w", encoding=self.encoding, newline=self.newline) as f:
                    f.write(yaml_safe_dump(group_rows))


_TABLEWRITER_CLASS_BY_FORMAT: MappingProxyType[str, type[TableFileWriter]] = MappingProxyType(
    {w.format: w for w in TableFileWriter.__subclasses__()}  # type: ignore[type-abstract]
)
