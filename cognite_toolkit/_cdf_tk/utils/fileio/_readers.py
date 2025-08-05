from abc import ABC, abstractmethod
from collections.abc import Iterator, Mapping
from io import TextIOWrapper
from pathlib import Path

from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses
from cognite_toolkit._cdf_tk.utils.collection import humanize_collection
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from ._base import FileIO
from ._compression import COMPRESSION_BY_SUFFIX, Compression


class FileReader(FileIO, ABC):
    def __init__(self, input_file: Path) -> None:
        self.input_file = input_file

    def read_chunks(self) -> Iterator[JsonVal]:
        compression = Compression.from_filepath(self.input_file)
        with compression.open("r") as file:
            yield from self._read_chunks_from_file(file)

    @abstractmethod
    def _read_chunks_from_file(self, file: TextIOWrapper) -> Iterator[JsonVal]:
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


FILE_READ_CLS_BY_FORMAT: Mapping[str, type[FileReader]] = {}
for subclass in get_concrete_subclasses(FileReader):  # type: ignore[type-abstract]
    if subclass.format in FILE_READ_CLS_BY_FORMAT:
        raise TypeError(
            f"Duplicate file format {subclass.format!r} found for classes "
            f"{FILE_READ_CLS_BY_FORMAT[subclass.format].__name__!r} and {subclass.__name__!r}."
        )
    # We know we have a dict, but we want to expose FILE_READ_CLS_BY_FORMAT as a Mapping
    FILE_READ_CLS_BY_FORMAT[subclass.format] = subclass  # type: ignore[index]
