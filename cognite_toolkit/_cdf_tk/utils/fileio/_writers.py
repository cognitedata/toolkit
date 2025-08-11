import sys
from abc import ABC, abstractmethod
from collections import Counter
from collections.abc import Iterable, Mapping
from pathlib import Path
from types import TracebackType
from typing import Generic

from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses
from cognite_toolkit._cdf_tk.utils.collection import humanize_collection
from cognite_toolkit._cdf_tk.utils.file import to_directory_compatible

from ._base import T_IO, Chunk, FileIO
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

    def write_chunks(self, chunks: Iterable[Chunk], filestem: str = "") -> None:
        filepath = self._get_filepath(filestem)
        writer = self._get_writer(filepath, filestem)
        self._write(writer, chunks)

    def _get_filepath(self, filename: str) -> Path:
        sanitized_name = f"{to_directory_compatible(filename)}-" if filename else ""
        file_count = self._file_count_by_filename[filename]
        file_path = (
            self.output_dir
            / f"{sanitized_name}part-{file_count:04}.{self.kind}{self.format}{self.compression_cls.file_suffix}"
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

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
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
        compression: type[Compression],
    ) -> "FileWriter":
        if format in FILE_WRITE_CLS_BY_FORMAT:
            file_writs_cls = FILE_WRITE_CLS_BY_FORMAT[format]
            return file_writs_cls(output_dir=output_dir, kind=kind, compression=compression)
        raise ToolkitValueError(
            f"Unknown file format: {format}. Available formats: {humanize_collection(FILE_WRITE_CLS_BY_FORMAT.keys())}."
        )


FILE_WRITE_CLS_BY_FORMAT: Mapping[str, type[FileWriter]] = {}

for subclass in get_concrete_subclasses(FileWriter):  # type: ignore[type-abstract]
    if subclass.format in FILE_WRITE_CLS_BY_FORMAT:
        raise TypeError(
            f"Duplicate file format {subclass.format!r} found for classes "
            f"{FILE_WRITE_CLS_BY_FORMAT[subclass.format].__name__!r} and {subclass.__name__!r}."
        )
    # We know we have a dict, but we want to expose FILE_WRITE_CLS_BY_FORMAT as a Mapping
    FILE_WRITE_CLS_BY_FORMAT[subclass.format] = subclass  # type: ignore[index]
