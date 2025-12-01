import csv
import json
import re
from abc import ABC, abstractmethod
from collections import Counter, defaultdict
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from functools import cached_property, partial
from io import TextIOWrapper
from pathlib import Path
from typing import Any

import yaml

from cognite_toolkit._cdf_tk.exceptions import ToolkitFileNotFoundError, ToolkitValueError
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses
from cognite_toolkit._cdf_tk.utils.collection import humanize_collection
from cognite_toolkit._cdf_tk.utils.dtype_conversion import convert_str_to_data_type, infer_data_type_from_value
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from ._base import FileIO, SchemaColumn
from ._compression import COMPRESSION_BY_SUFFIX, Compression


class FileReader(FileIO, ABC):
    def __init__(self, input_file: Path) -> None:
        self.input_file = input_file

    def read_chunks(self) -> Iterator[dict[str, JsonVal]]:
        compression = Compression.from_filepath(self.input_file)
        with compression.open("r") as file:
            yield from self._read_chunks_from_file(file)

    def read_chunks_with_line_numbers(self) -> Iterator[tuple[int, dict[str, JsonVal]]]:
        """Read chunks from the file, yielding each chunk with its corresponding line number."""
        yield from enumerate(self.read_chunks(), start=1)

    @abstractmethod
    def _read_chunks_from_file(self, file: TextIOWrapper) -> Iterator[dict[str, JsonVal]]:
        """Read chunks from the file."""
        ...

    @classmethod
    def from_filepath(cls, filepath: Path) -> "type[FileReader]":
        if len(filepath.suffixes) == 0:
            raise ToolkitValueError(
                f"File has no suffix. Available formats: {humanize_collection(FILE_READ_CLS_BY_FORMAT.keys())}."
            )
        suffix = filepath.suffix
        if suffix in COMPRESSION_BY_SUFFIX:
            if len(filepath.suffixes) > 1:
                suffix = filepath.suffixes[-2]
            else:
                raise ToolkitValueError(
                    f"File has a compression suffix, but no file format suffix found. Available formats: {humanize_collection(COMPRESSION_BY_SUFFIX.keys())}."
                )

        if suffix in FILE_READ_CLS_BY_FORMAT:
            return FILE_READ_CLS_BY_FORMAT[suffix]

        raise ToolkitValueError(
            f"Unknown file format: {suffix}. Available formats: {humanize_collection(FILE_READ_CLS_BY_FORMAT.keys())}."
        )

    @abstractmethod
    def count(self) -> int:
        """Count the number of chunks in the file."""
        ...


class MultiFileReader(FileReader):
    """Reads multiple files and yields chunks from each file sequentially.

    Args:
        input_files (Sequence[Path]): The list of file paths to read.
    """

    PART_PATTERN = re.compile(r"part-(\d{4})$")

    def __init__(self, input_files: Sequence[Path]) -> None:
        super().__init__(input_file=input_files[0])
        self.input_files = input_files

    @cached_property
    def reader_class(self) -> type[FileReader]:
        """Determine the reader class based on the input files."""
        reader_classes = Counter([FileReader.from_filepath(input_file) for input_file in self.input_files])
        if len(reader_classes) > 1:
            raise ToolkitValueError(
                "All input files must be of the same format. "
                f"Found formats: {humanize_collection([cls.FORMAT for cls in reader_classes.keys()])}."
            )
        return reader_classes.most_common(1)[0][0]

    @property
    def is_table(self) -> bool:
        try:
            return issubclass(self.reader_class, TableReader)
        except ValueError:
            # The input files are not a known format, so it is not a table.
            return False

    @property
    def format(self) -> str:
        return self.reader_class.FORMAT

    def read_chunks(self) -> Iterator[dict[str, JsonVal]]:
        for input_file in sorted(self.input_files, key=self._part_no):
            yield from self.reader_class(input_file).read_chunks()

    def _part_no(self, path: Path) -> int:
        match = self.PART_PATTERN.search(path.stem)
        if match:
            return int(match.group(1))
        return 99999

    def _read_chunks_from_file(self, file: TextIOWrapper) -> Iterator[dict[str, JsonVal]]:
        raise NotImplementedError("This method is not used in MultiFileReader.")

    def count(self) -> int:
        """Count the total number of chunks in all files."""
        total_count = 0
        for input_file in self.input_files:
            reader = self.reader_class(input_file)
            total_count += reader.count()
        return total_count


class NDJsonReader(FileReader):
    FORMAT = ".ndjson"

    def _read_chunks_from_file(self, file: TextIOWrapper) -> Iterator[dict[str, JsonVal]]:
        for line in file:
            if stripped := line.strip():
                yield json.loads(stripped)

    def count(self) -> int:
        """Count the number of lines (chunks) in the NDJSON file."""
        compression = Compression.from_filepath(self.input_file)
        with compression.open("r") as file:
            line_count = sum(1 for line in file if line.strip())
        return line_count


class YAMLBaseReader(FileReader, ABC):
    def _read_chunks_from_file(self, file: TextIOWrapper) -> Iterator[dict[str, JsonVal]]:
        yield from yaml.safe_load_all(file)

    def count(self) -> int:
        """Count the number of documents (chunks) in the YAML file."""
        compression = Compression.from_filepath(self.input_file)
        with compression.open("r") as file:
            doc_count = sum(1 for _ in yaml.safe_load_all(file))
        return doc_count


class YAMLReader(YAMLBaseReader):
    FORMAT = ".yaml"


class YMLReader(YAMLBaseReader):
    FORMAT = ".yml"


@dataclass
class FailedParsing:
    row: int
    column: str
    value: str
    error: str


class TableReader(FileReader, ABC):
    """Reads table-like files and yields each row as a dictionary.

    Args:
        input_file (Path): The path to the table file to read.
        sniff_rows (int | None): Optional number of rows to sniff for
            schema detection. If None, no schema is detected. If a schema is sniffed
            from the first `sniff_rows` rows, it will be used to parse the table.
        schema (Sequence[SchemaColumn] | None): Optional schema to use for parsing.
            You can either provide a schema or use `sniff_rows` to detect it.
        keep_failed_cells (bool): If True, failed cells will be kept in the
            `failed_cell` attribute. If False, they will be ignored.
    """

    def __init__(
        self,
        input_file: Path,
        sniff_rows: int | None = None,
        schema: Sequence[SchemaColumn] | None = None,
        keep_failed_cells: bool = False,
    ) -> None:
        super().__init__(input_file)
        if sniff_rows is not None and schema is not None:
            raise ValueError("Cannot specify both `sniff_rows` and `schema`. Use one or the other.")
        elif sniff_rows is not None and schema is None:
            self.schema: Sequence[SchemaColumn] | None = self.sniff_schema(input_file, sniff_rows)
        else:
            self.schema = schema
        self.sniff_rows = sniff_rows
        self.keep_failed_cells = keep_failed_cells
        self.parse_function_by_column = self._create_parse_functions(self.schema)
        self.failed_cell: list[FailedParsing] = []

    @classmethod
    def _create_parse_functions(
        cls, schema: Sequence[SchemaColumn] | None
    ) -> dict[str, Callable[[str | None], JsonVal]]:
        """Create a dictionary of parse functions for each column in the schema."""
        parse_function_by_column: dict[str, Callable[[str | None], JsonVal]] = defaultdict(
            lambda: cls._default_parse_function
        )
        if schema is not None:
            for column in schema:
                if column.type in {"date", "timestamp"}:
                    raise ToolkitValueError("CSVReader does not support 'date' or 'timestamp' types.")
                parse_function_by_column[column.name] = partial(  # type: ignore[assignment]
                    convert_str_to_data_type, type_=column.type, nullable=True, is_array=False
                )
        return parse_function_by_column

    @staticmethod
    def _default_parse_function(value: str | None) -> JsonVal:
        if value is None:
            return None
        return infer_data_type_from_value(value, "Json")[1]

    @classmethod
    def sniff_schema(cls, input_file: Path, sniff_rows: int = 100) -> list[SchemaColumn]:
        """
        Sniff the schema from the first `sniff_rows` rows of the file.

        Args:
            input_file (Path): The path to the tabular file.
            sniff_rows (int): The number of rows to read for sniffing the schema.

        Returns:
            list[SchemaColumn]: The inferred schema as a list of SchemaColumn objects.

        Raises:
            ValueError: If `sniff_rows` is not a positive integer.
            ToolkitFileNotFoundError: If the file does not exist.
            ToolkitValueError: If the file is not the correct format or if there are issues with the content.

        """
        if sniff_rows <= 0:
            raise ValueError("`sniff_rows` must be a positive integer.")

        if not input_file.exists():
            raise ToolkitFileNotFoundError(f"File not found: {input_file.as_posix()!r}.")
        if input_file.suffix != cls.FORMAT:
            raise ToolkitValueError(f"Expected a {cls.FORMAT} file got a {input_file.suffix!r} file instead.")

        column_names, sample_rows = cls._read_sample_rows(input_file, sniff_rows)
        cls._check_column_names(column_names)
        return cls._infer_schema(sample_rows, column_names)

    @classmethod
    @abstractmethod
    def _read_sample_rows(cls, input_file: Path, sniff_rows: int) -> tuple[Sequence[str], list[dict[str, str]]]: ...

    @classmethod
    def _infer_schema(cls, sample_rows: list[dict[str, Any]], column_names: Sequence[str]) -> list[SchemaColumn]:
        schema: list[SchemaColumn] = []
        for column_name in column_names:
            sample_values = [row[column_name] for row in sample_rows if column_name in row]
            if not sample_values:
                column = SchemaColumn(name=column_name, type="string")
            else:
                data_types = Counter(
                    infer_data_type_from_value(value, dtype="Json")[0] for value in sample_values if value is not None
                )
                if not data_types:
                    inferred_type = "string"
                else:
                    inferred_type = data_types.most_common()[0][0]
                # Json dtype is a subset of Datatype that SchemaColumn accepts
                column = SchemaColumn(name=column_name, type=inferred_type)  # type: ignore[arg-type]
            schema.append(column)
        return schema

    @classmethod
    def _check_column_names(cls, column_names: Sequence[str]) -> None:
        """Check for duplicate column names."""
        duplicates = [name for name, count in Counter(column_names).items() if count > 1]
        if duplicates:
            raise ToolkitValueError(f"Duplicate column names found: {humanize_collection(duplicates)}.")


class CSVReader(TableReader):
    """Reads CSV files and yields each row as a dictionary."""

    FORMAT = ".csv"

    def _read_chunks_from_file(self, file: TextIOWrapper) -> Iterator[dict[str, JsonVal]]:
        if self.keep_failed_cells and self.failed_cell:
            self.failed_cell.clear()
        for row_no, row in enumerate(csv.DictReader(file), start=1):
            parsed: dict[str, JsonVal] = {}
            for key, value in row.items():
                if value == "":
                    parsed[key] = None
                    continue
                try:
                    parsed[key] = self.parse_function_by_column[key](value)
                except ValueError as e:
                    parsed[key] = None
                    if self.keep_failed_cells:
                        self.failed_cell.append(FailedParsing(row=row_no, column=key, value=value, error=str(e)))
            yield parsed

    def read_chunks_unprocessed(self) -> Iterator[dict[str, str]]:
        """Read chunks from the CSV file without parsing values."""
        compression = Compression.from_filepath(self.input_file)
        with compression.open("r") as file:
            yield from csv.DictReader(file)

    @classmethod
    def _read_sample_rows(cls, input_file: Path, sniff_rows: int) -> tuple[Sequence[str], list[dict[str, str]]]:
        column_names: Sequence[str] = []
        compression = Compression.from_filepath(input_file)
        with compression.open("r") as file:
            reader = csv.DictReader(file)
            column_names = reader.fieldnames or []
            sample_rows: list[dict[str, str]] = []
            for no, row in enumerate(reader):
                if no >= sniff_rows:
                    break
                sample_rows.append(row)

            if not sample_rows:
                raise ToolkitValueError(f"No data found in the file: {input_file.as_posix()!r}.")
        return column_names, sample_rows

    def count(self) -> int:
        """Count the number of rows in the CSV file."""
        compression = Compression.from_filepath(self.input_file)
        with compression.open("r") as file:
            line_count = sum(1 for _ in file) - 1  # Subtract 1 for header
        return line_count


class ParquetReader(TableReader):
    FORMAT = ".parquet"

    def __init__(self, input_file: Path) -> None:
        # Parquet files have their own schema, so we don't need to sniff or provide one.
        super().__init__(input_file, sniff_rows=None, schema=None, keep_failed_cells=False)

    def read_chunks(self) -> Iterator[dict[str, JsonVal]]:
        import pyarrow.parquet as pq

        with pq.ParquetFile(self.input_file) as parquet_file:
            for batch in parquet_file.iter_batches():
                for chunk in batch.to_pylist():
                    yield {key: self._parse_value(value) for key, value in chunk.items()}

    def _read_chunks_from_file(self, file: TextIOWrapper) -> Iterator[dict[str, JsonVal]]:
        raise NotImplementedError(
            "This is not used by ParquetReader, as it reads directly from the file using pyarrow."
        )

    @staticmethod
    def _parse_value(value: JsonVal) -> JsonVal:
        """Parse a string value into its appropriate type."""
        if isinstance(value, str) and value:
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value
        return value

    @classmethod
    def _read_sample_rows(cls, input_file: Path, sniff_rows: int) -> tuple[Sequence[str], list[dict[str, str]]]:
        import pyarrow.parquet as pq

        column_names: Sequence[str] = []
        sample_rows: list[dict[str, str]] = []
        with pq.ParquetFile(input_file) as parquet_file:
            column_names = parquet_file.schema.names
            row_count = min(sniff_rows, parquet_file.metadata.num_rows)
            row_iter = parquet_file.iter_batches(batch_size=row_count)
            try:
                batch = next(row_iter)
                for row in batch.to_pylist():
                    str_row = {key: (str(value) if value is not None else "") for key, value in row.items()}
                    sample_rows.append(str_row)
            except StopIteration:
                pass

            if not sample_rows:
                raise ToolkitValueError(f"No data found in the file: {input_file.as_posix()!r}.")
        return column_names, sample_rows

    def count(self) -> int:
        """Count the number of rows in the Parquet file."""
        import pyarrow.parquet as pq

        with pq.ParquetFile(self.input_file) as parquet_file:
            return parquet_file.metadata.num_rows


FILE_READ_CLS_BY_FORMAT: Mapping[str, type[FileReader]] = {}
TABLE_READ_CLS_BY_FORMAT: Mapping[str, type[TableReader]] = {}
for subclass in get_concrete_subclasses(FileReader):  # type: ignore[type-abstract]
    if not getattr(subclass, "FORMAT", None):
        continue
    if subclass.FORMAT in FILE_READ_CLS_BY_FORMAT:
        raise TypeError(
            f"Duplicate file format {subclass.FORMAT!r} found for classes "
            f"{FILE_READ_CLS_BY_FORMAT[subclass.FORMAT].__name__!r} and {subclass.__name__!r}."
        )
    # We know we have a dict, but we want to expose FILE_READ_CLS_BY_FORMAT as a Mapping
    FILE_READ_CLS_BY_FORMAT[subclass.FORMAT] = subclass  # type: ignore[index]
    if issubclass(subclass, TableReader):
        if subclass.FORMAT in TABLE_READ_CLS_BY_FORMAT:
            raise TypeError(
                f"Duplicate table file format {subclass.FORMAT!r} found for classes "
                f"{TABLE_READ_CLS_BY_FORMAT[subclass.FORMAT].__name__!r} and {subclass.__name__!r}."
            )
        TABLE_READ_CLS_BY_FORMAT[subclass.FORMAT] = subclass  # type: ignore[index]
