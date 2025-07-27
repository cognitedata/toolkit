import gzip
import json
import sys
from abc import ABC
from collections.abc import Iterator
from io import TextIOWrapper
from pathlib import Path
from typing import ClassVar, Literal

from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class _RecordIO(ABC):
    encoding = "utf-8"
    newline = "\n"
    _file_mode: ClassVar[str]

    def __init__(
        self,
        filepath: Path,
        format: Literal["infer", "ndjson"],
        compression: Literal["infer", "gzip", "none"],
    ):
        self.filepath = filepath
        self.file: gzip.GzipFile | TextIOWrapper | None = None
        self.format = self.validate_format(format, filepath)
        self.compression = self.validate_compression(compression, filepath)

    @classmethod
    def validate_format(cls, format: str, filepath: Path) -> Literal["ndjson"]:
        """Validate the format based on the file extension."""
        if format == "infer":
            if len(filepath.suffixes) > 0 and filepath.suffixes[0] == ".ndjson":
                return "ndjson"
            else:
                raise ToolkitValueError(
                    f"Cannot infer format from file extension: {filepath.suffix}. Only '.ndjson' is supported."
                )
        elif format == "ndjson" and (len(filepath.suffixes) > 0 and filepath.suffixes[0] == ".ndjson"):
            return "ndjson"
        elif format == "ndjson":
            raise ToolkitValueError(f"Invalid format for file: {filepath}. Expected '.ndjson' suffix.")
        else:
            raise ToolkitValueError(f"Unsupported format: {format}")

    @classmethod
    def validate_compression(cls, compression: str, filepath: Path) -> Literal["gzip", "none"]:
        """Validate the compression type based on the file extension."""
        if compression == "infer":
            if filepath.suffix == ".gz":
                return "gzip"
            elif len(filepath.suffixes) == 1:
                return "none"
            else:
                raise ToolkitValueError(f"Cannot infer compression from filename: {filepath.name!r}")
        elif compression == "gzip" and filepath.suffix == ".gz":
            return compression  # type: ignore[return-value]
        elif compression == "gzip":
            raise ToolkitValueError(
                f"Invalid compression for file: {filepath.name!r}. Expected '.gz' suffix for gzip compression."
            )
        elif compression == "none" and len(filepath.suffixes) == 1:
            return compression  # type: ignore[return-value]
        elif compression == "none":
            raise ToolkitValueError(
                f"Invalid compression for file: {filepath.name!r}. Expected no suffix for no compression."
            )
        else:
            raise ToolkitValueError(f"Unsupported compression type: {compression}")

    def __enter__(self) -> Self:
        if self.compression == "gzip":
            self.file = gzip.open(self.filepath, f"{self._file_mode}t", encoding=self.encoding, newline=self.newline)
        elif self.compression == "none":
            self.file = self.filepath.open(self._file_mode, encoding=self.encoding, newline=self.newline)  # type: ignore[assignment]
        else:
            raise ValueError(f"Unsupported compression type: {self.compression}")
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: object | None
    ) -> None:
        if self.file:
            self.file.close()
            self.file = None


class RecordWriter(_RecordIO):
    _file_mode = "w"

    def __init__(
        self,
        filepath: Path,
        format: Literal["ndjson"] = "ndjson",
        compression: Literal["gzip", "none"] = "gzip",
    ) -> None:
        super().__init__(filepath, format, compression)

    def write_records(self, records: list[dict[str, JsonVal]]) -> None:
        """Write records to the file."""
        if not self.file:
            raise ToolkitValueError("File is not opened. Use 'with' statement to open the file.")

        self.file.writelines(
            # MyPY fails to understand list[str] is valid here.
            [f"{json.dumps(record)}{self.newline}" for record in records],  # type: ignore[misc]
        )


class RecordReader(_RecordIO):
    _file_mode = "r"
    """A class to read records from a file."""

    def __init__(self, filepath: Path) -> None:
        super().__init__(filepath, "infer", "infer")

    def read_records(self) -> Iterator[dict[str, JsonVal]]:
        """Read records from the file."""
        # Read records from the file
        if not self.file:
            raise ToolkitValueError("File is not opened. Use 'with' statement to open the file.")

        for line in self.file:
            yield json.loads(line.strip())
