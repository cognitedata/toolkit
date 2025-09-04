import csv
import importlib.util
import json
import sys
import threading
from abc import ABC, abstractmethod
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from datetime import date, datetime, timezone
from functools import lru_cache
from io import IOBase, TextIOWrapper
from pathlib import Path
from types import TracebackType
from typing import TYPE_CHECKING, Generic

import yaml

from cognite_toolkit._cdf_tk.exceptions import ToolkitMissingDependencyError, ToolkitTypeError, ToolkitValueError
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses
from cognite_toolkit._cdf_tk.utils.collection import humanize_collection
from cognite_toolkit._cdf_tk.utils.file import to_directory_compatible
from cognite_toolkit._cdf_tk.utils.table_writers import DataType

from ._base import T_IO, CellValue, Chunk, FileIO, SchemaColumn
from ._compression import Compression, Uncompressed

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


if TYPE_CHECKING:
    import pyarrow as pa
    import pyarrow.parquet as pq


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
        self._lock = threading.Lock()

    @property
    def file_count(self) -> int:
        """Get the total number of files written."""
        with self._lock:
            return len(self._writer_by_filepath)

    def write_chunks(self, chunks: Iterable[Chunk], filestem: str = "") -> None:
        with self._lock:
            filepath = self._get_filepath(filestem)
            writer = self._get_writer(filepath, filestem)
            self._write(writer, chunks)

    def _get_filepath(self, filename: str) -> Path:
        # This method is now called within the lock context from write_chunks
        sanitized_name = f"{to_directory_compatible(filename)}-" if filename else ""
        file_count = self._file_count_by_filename[filename]
        file_path = (
            self.output_dir
            / f"{sanitized_name}part-{file_count:04}.{self.kind}{self.format}{self.compression_cls.file_suffix}"
        )
        file_path.parent.mkdir(parents=True, exist_ok=True)
        return file_path

    def _get_writer(self, filepath: Path, filename_base: str) -> T_IO:
        # This method is now called within the lock context from write_chunks
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
        # Defensive check - should never trigger in normal usage
        if self._writer_by_filepath or self._file_count_by_filename:
            raise RuntimeError(f"{type(self).__name__} context manager should not be reused")
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        with self._lock:
            for writer in self._writer_by_filepath.values():
                writer.close()
            self._writer_by_filepath.clear()
            self._file_count_by_filename.clear()
            return None

    def _is_above_file_size_limit(self, filepath: Path, writer: T_IO) -> bool:
        """Check if the file size is above the limit."""
        try:
            writer.flush()
        except (AttributeError, ValueError):
            # Some writers might not support flush (e.g. already closed).
            # We can ignore this and proceed to check the file size on disk.
            pass
        return filepath.exists() and filepath.stat().st_size > self.max_file_size_bytes

    @abstractmethod
    def _create_writer(self, filepath: Path) -> T_IO:
        """Create a writer for the given file path."""
        raise NotImplementedError("This method should be implemented in subclasses.")

    @abstractmethod
    def _write(self, writer: T_IO, chunks: Iterable[Chunk]) -> None:
        """Write the chunk to the file."""
        raise NotImplementedError("This method should be implemented in subclasses.")

    @classmethod
    def create_from_format(
        cls,
        format: str,
        output_dir: Path,
        kind: str,
        compression: type[Compression] = Uncompressed,
        columns: Sequence[SchemaColumn] | None = None,
    ) -> "FileWriter":
        if format not in FILE_WRITE_CLS_BY_FORMAT:
            raise ToolkitValueError(
                f"Unknown file format: {format}. Available formats: {humanize_collection(FILE_WRITE_CLS_BY_FORMAT.keys())}."
            )
        file_writs_cls = FILE_WRITE_CLS_BY_FORMAT[format]
        if issubclass(file_writs_cls, TableWriter) and columns is not None:
            return file_writs_cls(output_dir=output_dir, kind=kind, compression=compression, columns=columns)
        elif issubclass(file_writs_cls, TableWriter):
            raise ToolkitValueError(f"File format {format} requires columns to be provided for table writers.")
        else:
            return file_writs_cls(output_dir=output_dir, kind=kind, compression=compression)


class TableWriter(FileWriter[T_IO], ABC):
    def __init__(
        self,
        output_dir: Path,
        kind: str,
        compression: type[Compression],
        columns: Sequence[SchemaColumn],
        max_file_size_bytes: int = 128 * 1024 * 1024,
    ) -> None:
        super().__init__(output_dir, kind, compression, max_file_size_bytes)
        self.columns = columns


class NDJsonWriter(FileWriter[TextIOWrapper]):
    format = ".ndjson"

    class _DateTimeEncoder(json.JSONEncoder):
        def default(self, obj: object) -> object:
            if isinstance(obj, date | datetime):
                return obj.isoformat()
            return super().default(obj)

    def _create_writer(self, filepath: Path) -> TextIOWrapper:
        """Create a writer for the given file path."""
        return self.compression_cls(filepath).open("w")

    def _write(self, writer: TextIOWrapper, chunks: Iterable[Chunk]) -> None:
        writer.writelines(
            f"{json.dumps(chunk, cls=self._DateTimeEncoder)}{self.compression_cls.newline}" for chunk in chunks
        )


class YAMLBaseWriter(FileWriter[TextIOWrapper], ABC):
    def _create_writer(self, filepath: Path) -> TextIOWrapper:
        return self.compression_cls(filepath).open("w")

    def _write(self, writer: TextIOWrapper, chunks: Iterable[Chunk]) -> None:
        yaml.safe_dump_all(chunks, writer, sort_keys=False, explicit_start=True)


class YAMLWriter(YAMLBaseWriter):
    format = ".yaml"


class YMLWriter(YAMLBaseWriter):
    format = ".yml"


class CSVWriter(TableWriter[TextIOWrapper]):
    format = ".csv"

    def __init__(
        self,
        output_dir: Path,
        kind: str,
        compression: type[Compression],
        columns: Sequence[SchemaColumn],
        max_file_size_bytes: int = 128 * 1024 * 1024,
    ) -> None:
        super().__init__(output_dir, kind, compression, columns, max_file_size_bytes)
        self._csvwriter_by_file: dict[TextIOWrapper, csv.DictWriter] = {}

    def _create_writer(self, filepath: Path) -> TextIOWrapper:
        file = self.compression_cls(filepath).open("w")
        self._csvwriter_by_file[file] = self._create_dict_writer(file)
        return file

    def _write(self, writer: TextIOWrapper, chunks: Iterable[Chunk]) -> None:
        try:
            csv_writer = self._csvwriter_by_file[writer]
        except KeyError:
            csv_writer = self._create_dict_writer(writer)
            self._csvwriter_by_file[writer] = csv_writer
        csv_writer.writerows(self._prepare_row(row) for row in chunks)

    @staticmethod
    def _prepare_row(row: Chunk) -> dict[str, str | int | float | bool]:
        """Prepare a row for writing to CSV."""
        prepared_row = {}
        value: str | int | float | bool
        for col, cell in row.items():
            if isinstance(cell, list | dict):
                value = json.dumps(cell, cls=NDJsonWriter._DateTimeEncoder)
            elif isinstance(cell, date | datetime):
                value = cell.isoformat()
            elif cell is None:
                value = ""
            elif isinstance(cell, int | float | bool):
                value = cell
            else:
                value = str(cell)
            prepared_row[col] = value
        return prepared_row

    def _create_dict_writer(self, writer: IOBase) -> csv.DictWriter:
        csv_writer = csv.DictWriter(writer, fieldnames=[col.name for col in self.columns], extrasaction="ignore")
        csv_writer.writeheader()
        return csv_writer


class ParquetWriter(TableWriter["pq.ParquetWriter"]):
    format = ".parquet"

    def _create_writer(self, filepath: Path) -> "pq.ParquetWriter":
        import pyarrow.parquet as pq

        schema = self._create_schema()
        return pq.ParquetWriter(filepath, schema)

    def _write(self, writer: "pq.ParquetWriter", chunks: Iterable[Chunk]) -> None:
        import pyarrow as pa

        json_columns = self._json_columns()
        timestamp_columns = self._timestamp_columns()
        date_columns = self._date_columns()
        if not json_columns and not timestamp_columns and not date_columns:
            # If no special columns, we can write directly without processing
            table = pa.Table.from_pylist(chunks, schema=self._create_schema())
            writer.write_table(table)
            return

        processed_chunks: list[Chunk] = []
        for chunk in chunks:
            # Create a copy to avoid mutating the input, which is an unexpected side-effect.
            processed_chunk = chunk.copy()
            for col, cell_value in processed_chunk.items():
                if col in json_columns:
                    processed_chunk[col] = json.dumps(cell_value)
                elif col in timestamp_columns:
                    if isinstance(cell_value, list):
                        # MyPy fails to recognize that list of datetime and date are valid CellValues.
                        processed_chunk[col] = [self._to_datetime(value) for value in cell_value]  # type: ignore[assignment]
                    else:
                        processed_chunk[col] = self._to_datetime(cell_value)
                elif col in date_columns:
                    if isinstance(cell_value, list):
                        processed_chunk[col] = [self._to_date(value) for value in cell_value]  # type: ignore[assignment]
                    else:
                        processed_chunk[col] = self._to_date(cell_value)
            processed_chunks.append(processed_chunk)

        if not processed_chunks:
            return

        table = pa.Table.from_pylist(processed_chunks, schema=self._create_schema())
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
        for prop in self.columns:
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


FILE_WRITE_CLS_BY_FORMAT: Mapping[str, type[FileWriter]] = {}
TABLE_WRITE_CLS_BY_FORMAT: Mapping[str, type[TableWriter]] = {}
for subclass in get_concrete_subclasses(FileWriter):  # type: ignore[type-abstract]
    if not getattr(subclass, "format", None):
        continue
    if subclass.format in FILE_WRITE_CLS_BY_FORMAT:
        raise TypeError(
            f"Duplicate file format {subclass.format!r} found for classes "
            f"{FILE_WRITE_CLS_BY_FORMAT[subclass.format].__name__!r} and {subclass.__name__!r}."
        )
    # We know we have a dict, but we want to expose FILE_WRITE_CLS_BY_FORMAT as a Mapping
    FILE_WRITE_CLS_BY_FORMAT[subclass.format] = subclass  # type: ignore[index]
    if issubclass(subclass, TableWriter):
        if subclass.format in TABLE_WRITE_CLS_BY_FORMAT:
            raise TypeError(
                f"Duplicate table file format {subclass.format!r} found for classes "
                f"{TABLE_WRITE_CLS_BY_FORMAT[subclass.format].__name__!r} and {subclass.__name__!r}."
            )
        TABLE_WRITE_CLS_BY_FORMAT[subclass.format] = subclass  # type: ignore[index]
