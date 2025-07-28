import gzip
import sys
from abc import ABC, abstractmethod
from collections import Counter
from collections.abc import Iterable, Iterator, Mapping
from datetime import date, datetime
from io import IOBase
from pathlib import Path
from typing import IO, Any, ClassVar, Generic, Literal, TypeAlias, TypeVar

from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError

from . import humanize_collection, to_directory_compatible
from .useful_types import JsonVal

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


PrimaryCellValue: TypeAlias = datetime | date | JsonVal
CellValue: TypeAlias = PrimaryCellValue | list[PrimaryCellValue]
Chunk: TypeAlias = dict[str, CellValue]


T_IO = TypeVar("T_IO", bound=IO)


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
        if filepath.suffix in _COMPRESSION_BY_SUFFIX:
            return _COMPRESSION_BY_SUFFIX[filepath.suffix](filepath=filepath)
        return NoneCompression(filepath=filepath)

    @classmethod
    def from_name(cls, compression: str) -> "Compression":
        if compression in _COMPRESSION_BY_NAME:
            return _COMPRESSION_BY_NAME[compression](filepath=Path("dummy"))
        raise ToolkitValueError(
            f"Unknown compression type: {compression}. Available types: {humanize_collection(_COMPRESSION_BY_NAME.keys())}."
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


class FileIO(ABC):
    format: ClassVar[str]


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
        cls, format: str, output_dir: Path, kind: str, compression: type[Compression]
    ) -> "FileWriter":
        if format in _FILE_WRITE_CLS_BY_FORMAT:
            return _FILE_WRITE_CLS_BY_FORMAT[format](output_dir=output_dir, kind=kind, compression=compression)
        raise ToolkitValueError(
            f"Unknown file format: {format}. Available formats: {humanize_collection(_FILE_WRITE_CLS_BY_FORMAT.keys())}."
        )


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
        if suffix in _COMPRESSION_BY_SUFFIX and len(filepath.suffix) > 1:
            suffix = filepath.suffix[-2]

        if suffix in _FILE_READ_CLS_BY_FORMAT:
            return _FILE_READ_CLS_BY_FORMAT[filepath.suffix](input_file=filepath)
        raise ToolkitValueError(
            f"Unknown file format: {filepath.suffix}. Available formats: {humanize_collection(_FILE_READ_CLS_BY_FORMAT.keys())}."
        )


_COMPRESSION_BY_SUFFIX: Mapping[str, type[Compression]] = {
    subclass.file_suffix: subclass  # type: ignore[type-abstract]
    for subclass in Compression.__subclasses__()
}
_COMPRESSION_BY_NAME: Mapping[str, type[Compression]] = {
    subclass.name: subclass  # type: ignore[type-abstract]
    for subclass in Compression.__subclasses__()
}

_FILE_READ_CLS_BY_FORMAT: Mapping[str, type[FileReader]] = {
    subclass.format: subclass  # type: ignore[type-abstract]
    for subclass in FileReader.__subclasses__()
}

_FILE_WRITE_CLS_BY_FORMAT: Mapping[str, type[FileWriter]] = {
    subclass.format: subclass  # type: ignore[type-abstract]
    for subclass in FileWriter.__subclasses__()
}
