import gzip
import sys
from abc import ABC, abstractmethod
from collections import Counter
from collections.abc import Iterable, Iterator
from datetime import date, datetime
from io import TextIOWrapper
from pathlib import Path
from typing import IO, Any, ClassVar, Generic, Literal, TypeAlias, TypeVar

from . import to_directory_compatible
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
    compression: ClassVar[str]
    file_suffix: ClassVar[str]

    def __init__(self, filepath: Path) -> None:
        self.filepath = filepath

    @abstractmethod
    def open(self, mode: Literal["r", "w"]) -> TextIOWrapper:
        """Open the compressed file and return a file-like object."""
        raise NotImplementedError("This method should be implemented in subclasses.")

    @classmethod
    def from_filepath(cls, filepath: Path) -> "Compression":
        raise NotImplementedError()

    @classmethod
    def from_compression(cls, compression: str, filepath: Path) -> "Compression":
        raise NotImplementedError("This method should be implemented in subclasses.")


class NoneCompression(Compression):
    compression = "none"
    file_suffix = ""

    def open(self, mode: Literal["r", "w"]) -> TextIOWrapper:
        """Open the file without compression."""
        return self.filepath.open(mode=mode, encoding=FileIO.encoding, newline=FileIO.newline)


class GzipCompression(Compression):
    compression = "gzip"
    file_suffix = ".gz"

    def open(self, mode: Literal["r", "w"]) -> TextIOWrapper:
        """Open the gzip compressed file."""
        return gzip.open(self.filepath, mode=f"{mode}t", encoding=FileIO.encoding, newline=FileIO.newline)  # type: ignore[return-value]


class FileIO(ABC, Generic[T_IO]):
    encoding = "utf-8"
    newline = "\n"
    format: ClassVar[str]


class FileWriter(FileIO[T_IO], ABC):
    def __init__(
        self, output_dir: Path, kind: str, compression: Compression, max_file_size_bytes: int = 128 * 1024 * 1024
    ) -> None:
        self.output_dir = output_dir
        self.kind = kind
        self.compression = compression
        self.max_file_size_bytes = max_file_size_bytes
        self._file_count_by_filename: dict[str, int] = Counter()
        self._writer_by_filepath: dict[Path, T_IO] = {}

    def write_chunk(self, chunk: Iterable[Chunk], filename: str = "") -> None:
        filepath = self._get_filepath(filename)
        writer = self._get_writer(filepath, filename)
        self._write(writer, chunk)

    def _get_filepath(self, filename: str) -> Path:
        clean_name = f"{to_directory_compatible(filename)}-" if filename else ""
        file_count = self._file_count_by_filename[filename]
        file_path = (
            self.output_dir
            / f"{clean_name}part-{file_count:04}.{self.kind}.{self.format}{self.compression.file_suffix}"
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
    def _write(self, writer: T_IO, chunk: Iterable[Chunk]) -> None:
        """Write the chunk to the file."""
        raise NotImplementedError("This method should be implemented in subclasses.")


class FileReader(FileIO, ABC):
    def __init__(self, input_path: Path, kind: str | None = None) -> None:
        self.input_path = input_path
        self.kind = kind
        self.validate_input(input_path, kind)

    @staticmethod
    def validate_input(input_path: Path, kind: str | None) -> None:
        if not input_path.exists():
            raise FileNotFoundError(f"The input path {input_path} does not exist.")
        if kind is not None and not isinstance(kind, str):
            raise ValueError("The 'kind' parameter must be a string or None.")

    def _input_files(self) -> Iterable[Path]:
        """Return an iterable of input files."""
        if self.input_path.is_dir():
            return self.input_path.glob(f"*.{self.kind}.{self.format}*")
        elif self.input_path.is_file():
            return [self.input_path]
        else:
            raise ValueError(f"The input path {self.input_path} is neither a file nor a directory.")

    def read_chunks(self) -> Iterator[JsonVal]:
        for input_file in self._input_files():
            compression = Compression.from_filepath(input_file)
            with compression.open("r") as file:
                yield from self._read_chunks_from_file(file)

    @abstractmethod
    def _read_chunks_from_file(self, file: TextIOWrapper) -> Iterator[JsonVal]:
        """Read chunks from the file."""
        raise NotImplementedError("This method should be implemented in subclasses.")
