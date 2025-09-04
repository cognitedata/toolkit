import gzip
from abc import ABC, abstractmethod
from collections.abc import Mapping
from io import TextIOWrapper
from pathlib import Path
from typing import ClassVar, Literal

from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses


class Compression(ABC):
    encoding = "utf-8-sig"
    newline = "\n"
    name: ClassVar[str]
    file_suffix: ClassVar[str]

    def __init__(self, filepath: Path) -> None:
        self.filepath = filepath

    @abstractmethod
    def open(self, mode: Literal["r", "w"]) -> TextIOWrapper:
        """Open the compressed file and return a file-like object."""
        raise NotImplementedError("This method should be implemented in subclasses.")

    @classmethod
    def from_filepath(cls, filepath: Path) -> "Compression":
        return COMPRESSION_BY_SUFFIX.get(filepath.suffix, Uncompressed)(filepath=filepath)

    @classmethod
    def from_name(cls, compression: str) -> "type[Compression]":
        if compression in COMPRESSION_BY_NAME:
            return COMPRESSION_BY_NAME[compression]
        raise ToolkitValueError(
            f"Unknown compression type: {compression}. Available types: {humanize_collection(COMPRESSION_BY_NAME.keys())}."
        )


class Uncompressed(Compression):
    name = "none"
    file_suffix = ""

    def open(self, mode: Literal["r", "w"]) -> TextIOWrapper:
        """Open the file without compression."""
        return self.filepath.open(mode=mode, encoding=self.encoding, newline=self.newline)


class GzipCompression(Compression):
    name = "gzip"
    file_suffix = ".gz"

    def open(self, mode: Literal["r", "w"]) -> TextIOWrapper:
        """Open the gzip compressed file."""
        # MyPy (or gzip) fails to recognize that gzip.open returns a TextIOWrapper
        return gzip.open(self.filepath, mode=f"{mode}t", encoding=self.encoding, newline=self.newline)  # type: ignore[return-value]


_all_compression_types: list[type[Compression]] = get_concrete_subclasses(Compression)  # type: ignore[type-abstract]
COMPRESSION_BY_SUFFIX: Mapping[str, type[Compression]] = {x.file_suffix: x for x in _all_compression_types}
COMPRESSION_BY_NAME: Mapping[str, type[Compression]] = {x.name: x for x in _all_compression_types}
