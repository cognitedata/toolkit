
import gzip
from abc import ABC, abstractmethod
from collections.abc import Mapping
from io import IOBase
from pathlib import Path
from typing import ClassVar, Literal

from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils._auxillery import get_get_concrete_subclasses


class Compression(ABC):
    encoding = "utf-8"
    newline = "\n"
    name: ClassVar[str]
    file_suffix: ClassVar[str]

    def __init__(self, filepath: Path) -> None:
        self.filepath = filepath

    @abstractmethod
    def open(self, mode: Literal["r", "w"]) -> IOBase:
        """Open the compressed file and return a file-like object."""
        raise NotImplementedError("This method should be implemented in subclasses.")

    @classmethod
    def from_filepath(cls, filepath: Path) -> "Compression":
        if filepath.suffix in COMPRESSION_BY_SUFFIX:
            return COMPRESSION_BY_SUFFIX[filepath.suffix](filepath=filepath)
        return NoneCompression(filepath=filepath)

    @classmethod
    def from_name(cls, compression: str) -> "Compression":
        if compression in COMPRESSION_BY_NAME:
            return COMPRESSION_BY_NAME[compression](filepath=Path("dummy"))
        raise ToolkitValueError(
            f"Unknown compression type: {compression}. Available types: {humanize_collection(COMPRESSION_BY_NAME.keys())}."
        )


class NoneCompression(Compression):
    name = "none"
    file_suffix = ""

    def open(self, mode: Literal["r", "w"]) -> IOBase:
        """Open the file without compression."""
        return self.filepath.open(mode=mode, encoding=self.encoding, newline=self.newline)


class GzipCompression(Compression):
    name = "gzip"
    file_suffix = ".gz"

    def open(self, mode: Literal["r", "w"]) -> IOBase:
        """Open the gzip compressed file."""
        return gzip.open(self.filepath, mode=f"{mode}t", encoding=self.encoding, newline=self.newline)

COMPRESSION_BY_SUFFIX: Mapping[str, type[Compression]] = {
    subclass.file_suffix: subclass
    for subclass in get_get_concrete_subclasses(Compression)
}

COMPRESSION_BY_NAME: Mapping[str, type[Compression]] = {
    subclass.name: subclass  # type: ignore[type-abstract]
    for subclass in get_get_concrete_subclasses(Compression)
}