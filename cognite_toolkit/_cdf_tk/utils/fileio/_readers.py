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
from ._base import FileIO
from ._compression import Compression, NoneCompression, GzipCompression, COMPRESSION_BY_SUFFIX, COMPRESSION_BY_NAME


if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class FileReader(FileIO, ABC):
    def __init__(self, input_file: Path) -> None:
        self.input_file = input_file

    def read_chunks(self) -> Iterator[JsonVal]:
        compression = Compression.from_filepath(self.input_file)
        with compression.open("r") as file:
            yield from self._read_chunks_from_file(file)

    @abstractmethod
    def _read_chunks_from_file(self, file: IOBase) -> Iterator[JsonVal]:
        """Read chunks from the file."""
        raise NotImplementedError("This method should be implemented in subclasses.")

    @classmethod
    def from_filepath(cls, filepath: Path) -> "FileReader":
        suffix = filepath.suffix
        if suffix in COMPRESSION_BY_SUFFIX and len(filepath.suffixes) > 1:
            suffix = filepath.suffixes[-2]

        if suffix in _FILE_READ_CLS_BY_FORMAT:
            return _FILE_READ_CLS_BY_FORMAT[suffix](input_file=filepath)
        raise ToolkitValueError(
            f"Unknown file format: {filepath.suffix}. Available formats: {humanize_collection(_FILE_READ_CLS_BY_FORMAT.keys())}."
        )


class NDJsonReader(FileReader):
    format = ".ndjson"

    def _read_chunks_from_file(self, file: IOBase) -> Iterator[JsonVal]:
        for line in file:
            yield json.loads(line.strip(), object_hook=self._parse_datetime)

    @staticmethod
    def _parse_datetime(obj: dict) -> object:
        for key, value in obj.items():
            if isinstance(value, str):
                try:
                    # Try parsing as datetime
                    obj[key] = date.fromisoformat(value)
                except ValueError:
                    try:
                        # Try parsing as date
                        obj[key] = datetime.fromisoformat(value)
                    except ValueError:
                        pass
        return obj


class CSVReader(FileReader):
    format = ".csv"

    def _read_chunks_from_file(self, file: IOBase) -> Iterator[JsonVal]:
        reader = csv.DictReader()
        for row in reader:
            yield row


class ParquetReader(FileReader):
    ...



class YAMLReader(FileReader):
    ...


_FILE_READ_CLS_BY_FORMAT: Mapping[str, type[FileReader]] = {subclass.format: subclass for subclass in get_get_concrete_subclasses(FileReader)}


