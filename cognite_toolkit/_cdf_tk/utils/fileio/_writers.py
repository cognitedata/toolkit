import csv
import gzip
import importlib.util
import json
import sys
from abc import ABC, abstractmethod
from collections import Counter
from collections.abc import Iterable, Iterator, Mapping
from datetime import date, datetime, timezone
from functools import lru_cache
from io import IOBase, TextIOWrapper
from pathlib import Path
from typing import IO, Any, ClassVar, Literal, TypeAlias, TypeVar, Generic

from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError, ToolkitMissingDependencyError, ToolkitTypeError

from cognite_toolkit._cdf_tk.utils.collection import humanize_collection
from cognite_toolkit._cdf_tk.utils.file import to_directory_compatible
from cognite_toolkit._cdf_tk.utils.file import yaml_safe_dump
from cognite_toolkit._cdf_tk.utils.table_writers import SchemaColumnList, DataType
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal
from cognite_toolkit._cdf_tk.utils._auxillery import get_get_concrete_subclasses
from ._base import FileIO, T_IO, Chunk, CellValue
from ._compression import Compression

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class FileWriter(FileIO, ABC, Generic[T_IO]):
    def __init__(
        self, output_dir: Path, kind: str, compression: type[Compression], max_file_size_bytes: int = 128 * 1024 * 1024
    ) -> None:
        self.output_dir = output_dir
        self.kind = kind
        self.compression_cls = compression
        self.max_file_size_bytes = max_file_size_bytes
        self._file_count_by_filename: dict[str, int] = Counter()
        self._writer_by_filepath: dict[Path, T_IO] = {}

    def write_chunks(self, chunk: Iterable[Chunk], filename: str = "") -> None:
        filepath = self._get_filepath(filename)
        writer = self._get_writer(filepath, filename)
        self._write(writer, chunk)

    def _get_filepath(self, filename: str) -> Path:
        clean_name = f"{to_directory_compatible(filename)}-" if filename else ""
        file_count = self._file_count_by_filename[filename]
        file_path = (
            self.output_dir
            / f"{clean_name}part-{file_count:04}.{self.kind}.{self.format}{self.compression_cls.file_suffix}"
        )
        file_path.parent.mkdir(parents=True, exist_ok=True)
        return file_path

    def _get_writer(self, filepath: Path, filename_base: str) -> T_IO:
        if filepath not in self._writer_by_filepath:
            self._writer_by_filepath[filepath] = self._create_writer(filepath)
        elif self._is_above_file_size_limit(filepath, self._writer_by_filepath[filepath]):
            self._writer_by_filepath[filepath].close()
            del self._writer_by_filepath[filepath]
            self._file_count_by_filename[filename_base] += 1
            new_filepath = self._get_filepath(filename_base)
            return self._get_writer(new_filepath, filename_base)
        return self._writer_by_filepath[filepath]

    def __enter__(self) -> Self:
        self._file_count_by_filename.clear()
        self._writer_by_filepath.clear()
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any | None) -> None:
        for writer in self._writer_by_filepath.values():
            writer.close()
        self._writer_by_filepath.clear()
        self._file_count_by_filename.clear()
        return None

    @abstractmethod
    def _create_writer(self, filepath: Path) -> T_IO:
        """Create a writer for the given file path."""
        raise NotImplementedError("This method should be implemented in subclasses.")

    @abstractmethod
    def _is_above_file_size_limit(self, filepath: Path, writer: T_IO) -> bool:
        """Check if the file size is above the limit."""
        raise NotImplementedError("This method should be implemented in subclasses.")

    @abstractmethod
    def _write(self, writer: T_IO, chunks: Iterable[Chunk]) -> None:
        """Write the chunk to the file."""
        raise NotImplementedError("This method should be implemented in subclasses.")

    @classmethod
    def create_from_format(
        cls, format: str, output_dir: Path, kind: str, compression: type[Compression], columns: SchemaColumnList | None = None
    ) -> "FileWriter":
        if format in _FILE_WRITE_CLS_BY_FORMAT:
            file_writs_cls = _FILE_WRITE_CLS_BY_FORMAT[format]
            if issubclass(file_writs_cls, TableWriter) and columns is not None:
                return file_writs_cls(output_dir=output_dir, kind=kind, compression=compression, columns=columns)
            elif issubclass(file_writs_cls, TableWriter):
                raise ToolkitValueError(
                    f"File format {format} requires columns to be provided for table writers."
                )
            else:
                return file_writs_cls(output_dir=output_dir, kind=kind, compression=compression)
        raise ToolkitValueError(
            f"Unknown file format: {format}. Available formats: {humanize_collection(_FILE_WRITE_CLS_BY_FORMAT.keys())}."
        )


class TableWriter(FileWriter, ABC):
    def __init__(
        self,
        output_dir: Path,
        kind: str,
        compression: type[Compression],
        columns: SchemaColumnList,
        max_file_size_bytes: int = 128 * 1024 * 1024,
    ) -> None:
        super().__init__(output_dir, kind, compression, max_file_size_bytes)
        self.columns = columns


class NDJsonWriter(FileWriter):
    format = ".ndjson"

    class _DateTimeEncoder(json.JSONEncoder):
        def default(self, obj: object) -> object:
            if isinstance(obj, date | datetime):
                return obj.isoformat()
            return super().default(obj)

    def _create_writer(self, filepath: Path) -> IOBase:
        compression = self.compression_cls(filepath)
        return compression.open("w")

    def _is_above_file_size_limit(self, filepath: Path, writer: IOBase) -> bool:
        return filepath.stat().st_size > self.max_file_size_bytes

    def _write(self, writer: IOBase, chunks: Iterable[Chunk]) -> None:
        writer.writelines(
            f"{json.dumps(chunk, cls=self._DateTimeEncoder)}{self.compression_cls.newline}"  # type: ignore[misc]
            for chunk in chunks
        )


class CSVWriter(TableWriter[csv.DictWriter]):
    format = ".csv"

    def _create_writer(self, filepath: Path) -> csv.DictWriter:
        file = self.compression_cls(filepath).open("w")
        writer = csv.DictWriter(file, fieldnames=[col.name for col in self.columns], extrasaction="ignore")
        if filepath.stat().st_size == 0:
            writer.writeheader()
        return writer

    def _is_above_file_size_limit(self, filepath: Path, writer: csv.DictWriter) -> bool:
        current_position = writer.writer.tell()
        writer.writer.seek(0, 2)
        if writer.writer.tell() > self.max_file_size_bytes:
            return True
        writer.writer.seek(current_position)
        return False

    def _write(self, writer: csv.DictWriter, chunks: Iterable[Chunk]) -> None:
        writer.writerows(chunks)

    def _create_dict_writer(self, writer: TextIOWrapper) -> csv.DictWriter:
        return csv.DictWriter(writer, fieldnames=[col.name for col in self.columns], extrasaction="ignore")



class ParquetWriter(TableWriter["pq.ParquetWriter"]):
    format = ".parquet"

    def _create_writer(self, filepath: Path) -> "pq.ParquetWriter":
        import pyarrow.parquet as pq

        schema = self._create_schema()
        return pq.ParquetWriter(filepath, schema)

    def _is_above_file_size_limit(self, filepath: Path, writer: "pq.ParquetWriter") -> bool:
        return filepath.exists() and filepath.stat().st_size > self.max_file_size_bytes

    def _write(self, writer: "pq.ParquetWriter", chunks: Iterable[Chunk]) -> None:
        import pyarrow as pa

        if json_columns := self._json_columns():
            for row in chunks:
                json_values = set(row.keys()) & json_columns
                for col in json_values:
                    row[col] = json.dumps(row[col])
        if timestamp_columns := self._timestamp_columns():
            for row in chunks:
                for col in set(row.keys()) & timestamp_columns:
                    cell_value = row[col]
                    if isinstance(cell_value, list):
                        # MyPy does not understand that a list of PrimaryCellValue is valid here
                        # It expects a union of PrimaryCellValue and list[PrimaryCellValue].
                        row[col] = [self._to_datetime(value) for value in cell_value]  # type: ignore[assignment]
                    else:
                        row[col] = self._to_datetime(cell_value)
        if date_columns := self._date_columns():
            for row in chunks:
                for col in set(row.keys()) & date_columns:
                    cell_value = row[col]
                    if isinstance(cell_value, list):
                        # MyPy does not understand that a list of PrimaryCellValue is valid here.
                        # It expects a union of PrimaryCellValue and list[PrimaryCellValue].
                        row[col] = [self._to_date(value) for value in cell_value]  # type: ignore[assignment]
                    else:
                        row[col] = self._to_date(cell_value)

        table = pa.Table.from_pylist(chunks, schema=self._create_schema())
        writer.write_table(table)

    @lru_cache(maxsize=1)
    def _json_columns(self) -> set[str]:
        """Check if the writer supports JSON format."""
        return {col.name for col in self.columns if col.type == "json"}

    @lru_cache(maxsize=1)
    def _timestamp_columns(self) -> set[str]:
        """Check if the writer supports timestamp format."""
        return {col.name for col in self.columns if col.type == "timestamp"}

    @lru_cache(maxsize=1)
    def _date_columns(self) -> set[str]:
        return {col.name for col in self.columns if col.type == "date"}

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

class YAMLWriter(FileWriter[IOBase]):
    def _create_writer(self, filepath: Path) -> IOBase:
        return self.compression_cls(filepath).open("w")

    def _is_above_file_size_limit(self, filepath: Path, writer: IOBase) -> bool:
        current_position = writer.tell()
        writer.seek(0, 2)
        if writer.tell() > self.max_file_size_bytes:
            return True
        writer.seek(current_position)
        return False

    def _write(self, writer: IOBase, chunks: Iterable[Chunk]) -> None:
        writer.write(yaml_safe_dump(chunks))



class YAML1Writer(YAMLWriter):
    format = ".yaml"

class YAML2Writer(YAMLWriter):
    format = ".yml"


_FILE_WRITE_CLS_BY_FORMAT: Mapping[str, type[FileWriter]] = {subclass.format: subclass for subclass in get_get_concrete_subclasses(FileWriter)}