import csv
import json
from abc import ABC, abstractmethod
from collections import Counter, defaultdict
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from functools import partial
from io import TextIOWrapper
from pathlib import Path

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

    @abstractmethod
    def _read_chunks_from_file(self, file: TextIOWrapper) -> Iterator[dict[str, JsonVal]]:
        """Read chunks from the file."""
        raise NotImplementedError("This method should be implemented in subclasses.")

    @classmethod
    def from_filepath(cls, filepath: Path) -> "FileReader":
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
            return FILE_READ_CLS_BY_FORMAT[suffix](input_file=filepath)

        raise ToolkitValueError(
            f"Unknown file format: {suffix}. Available formats: {humanize_collection(FILE_READ_CLS_BY_FORMAT.keys())}."
        )


class NDJsonReader(FileReader):
    format = ".ndjson"

    def _read_chunks_from_file(self, file: TextIOWrapper) -> Iterator[dict[str, JsonVal]]:
        for line in file:
            if stripped := line.strip():
                yield json.loads(stripped)


class YAMLBaseReader(FileReader, ABC):
    def _read_chunks_from_file(self, file: TextIOWrapper) -> Iterator[dict[str, JsonVal]]:
        yield from yaml.safe_load_all(file)


class YAMLReader(YAMLBaseReader):
    format = ".yaml"


class YMLReader(YAMLBaseReader):
    format = ".yml"


@dataclass
class FailedParsing:
    row: int
    column: str
    value: str
    error: str


class CSVReader(FileReader):
    """Reads CSV files and yields each row as a dictionary.

    Args:
        input_file (Path): The path to the CSV file to read.
        sniff_rows (int | None): Optional number of rows to sniff for
            schema detection. If None, no schema is detected. If a schema is sniffed
            from the first `sniff_rows` rows, it will be used to parse the CSV.
        schema (Sequence[SchemaColumn] | None): Optional schema to use for parsing.
            You can either provide a schema or use `sniff_rows` to detect it.
        keep_failed_cells (bool): If True, failed cells will be kept in the
            `failed_cell` attribute. If False, they will be ignored.

    """

    format = ".csv"

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
        Sniff the schema from the first `sniff_rows` rows of the CSV file.

        Args:
            input_file (Path): The path to the CSV file.
            sniff_rows (int): The number of rows to read for sniffing the schema.

        Returns:
            list[SchemaColumn]: The inferred schema as a list of SchemaColumn objects.
        Raises:
            ValueError: If `sniff_rows` is not a positive integer.
            ToolkitFileNotFoundError: If the file does not exist.
            ToolkitValueError: If the file is not a CSV file or if there are issues with the content.

        """
        if sniff_rows <= 0:
            raise ValueError("`sniff_rows` must be a positive integer.")

        if not input_file.exists():
            raise ToolkitFileNotFoundError(f"File not found: {input_file.as_posix()!r}.")
        if input_file.suffix != ".csv":
            raise ToolkitValueError(f"Expected a .csv file got a {input_file.suffix!r} file instead.")

        with input_file.open("r", encoding="utf-8-sig") as file:
            reader = csv.DictReader(file)
            column_names = Counter(reader.fieldnames)
            if duplicated := [name for name, count in column_names.items() if count > 1]:
                raise ToolkitValueError(f"CSV file contains duplicate headers: {humanize_collection(duplicated)}")
            sample_rows: list[dict[str, str]] = []
            for no, row in enumerate(reader):
                if no >= sniff_rows:
                    break
                sample_rows.append(row)

            if not sample_rows:
                raise ToolkitValueError(f"No data found in the file: {input_file.as_posix()!r}.")

            schema = []
            for column_name in reader.fieldnames or []:
                sample_values = [row[column_name] for row in sample_rows if column_name in row]
                if not sample_values:
                    column = SchemaColumn(name=column_name, type="string")
                else:
                    data_types = Counter(
                        infer_data_type_from_value(value, dtype="Json")[0]
                        for value in sample_values
                        if value is not None
                    )
                    if not data_types:
                        inferred_type = "string"
                    else:
                        inferred_type = data_types.most_common()[0][0]
                    # Json dtype is a subset of Datatype that SchemaColumn accepts
                    column = SchemaColumn(name=column_name, type=inferred_type)  # type: ignore[arg-type]
                schema.append(column)
        return schema

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


class ParquetReader(FileReader):
    format = ".parquet"

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


FILE_READ_CLS_BY_FORMAT: Mapping[str, type[FileReader]] = {}
for subclass in get_concrete_subclasses(FileReader):  # type: ignore[type-abstract]
    if not getattr(subclass, "format", None):
        continue
    if subclass.format in FILE_READ_CLS_BY_FORMAT:
        raise TypeError(
            f"Duplicate file format {subclass.format!r} found for classes "
            f"{FILE_READ_CLS_BY_FORMAT[subclass.format].__name__!r} and {subclass.__name__!r}."
        )
    # We know we have a dict, but we want to expose FILE_READ_CLS_BY_FORMAT as a Mapping
    FILE_READ_CLS_BY_FORMAT[subclass.format] = subclass  # type: ignore[index]
