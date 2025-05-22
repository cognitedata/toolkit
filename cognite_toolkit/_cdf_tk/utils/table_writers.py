import csv
from abc import abstractmethod
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, ClassVar, Literal, TypeAlias

from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.utils import humanize_collection, to_directory_compatible
from cognite_toolkit._cdf_tk.utils.file import yaml_safe_dump

FileFormat: TypeAlias = Literal["csv", "parquet", "yaml"]
Rows: TypeAlias = list[dict[str, Any]]


@dataclass
class SchemaColumn:
    name: str
    type: str


@dataclass
class Schema:
    display_name: str
    folder_name: str
    kind: str
    format_: FileFormat
    columns: list[SchemaColumn]


class FileWriter:
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
    def load(cls, schema: Schema, output_directory: Path) -> "FileWriter":
        write_cls = _TABLEWRITER_CLASS_BY_FORMAT.get(schema.format_)
        if write_cls is None:
            raise ToolkitValueError(
                f"Unsupported format {schema.format_}. Supported formats are {humanize_collection(_TABLEWRITER_CLASS_BY_FORMAT.keys())}."
            )

        return write_cls(schema, output_directory)


class ParquetWrite(FileWriter):
    format = "parquet"

    def write_rows(self, rows_group_list: list[tuple[str, Rows]]) -> None:
        pass


class CSVWriter(FileWriter):
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


class YAMLWriter(FileWriter):
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


_TABLEWRITER_CLASS_BY_FORMAT: MappingProxyType[str, type[FileWriter]] = MappingProxyType(
    {w.format: w for w in FileWriter.__subclasses__()}  # type: ignore[type-abstract]
)
