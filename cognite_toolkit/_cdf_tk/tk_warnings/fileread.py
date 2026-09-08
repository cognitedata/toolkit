from abc import ABC
from collections.abc import Hashable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar

from rich.markup import escape

from .base import SeverityLevel, ToolkitWarning


@dataclass(frozen=True)
class FileReadWarning(ToolkitWarning, ABC):
    severity: ClassVar[SeverityLevel]
    filepath: Path

    def group_key(self) -> tuple[Any, ...]:
        return (self.filepath,)

    def group_header(self) -> str:
        return f"    In File {self.filepath.as_posix()!r}"

    def __str__(self) -> str:
        return self.get_message()


@dataclass(frozen=True)
class FileExistsWarning(FileReadWarning):
    severity = SeverityLevel.MEDIUM
    extra_message: str = ""

    def get_message(self) -> str:
        msg = f"{type(self).__name__}: The file {self.filepath} already exists."
        if self.extra_message:
            msg += f" {self.extra_message}"
        return msg


@dataclass(frozen=True)
class IdentifiedResourceFileReadWarning(FileReadWarning, ABC):
    id_value: str
    id_name: str


@dataclass(frozen=True)
class YAMLFileWarning(FileReadWarning, ABC):
    def __post_init__(self) -> None:
        if self.filepath.suffix not in {".yaml", ".yml"}:
            raise ValueError(f"Expected a YAML file, got {self.filepath.suffix}.")


@dataclass(frozen=True)
class YAMLFileWithElementWarning(YAMLFileWarning, ABC):
    # None is a dictionary, number is a list
    element_no: int | None
    path: tuple[str | int, ...]

    @property
    def _location(self) -> str:
        if self.element_no is not None:
            value = f" in entry {self.element_no} "
        else:
            value = ""
        if len(self.path) <= 1:
            return f"{value}"
        else:
            return f"{value} in section {self.path!r}"


@dataclass(frozen=True)
class MissingReferencedWarning(YAMLFileWithElementWarning):
    message: str

    def get_message(self) -> str:
        return f"{type(self).__name__}: {self.message}."


@dataclass(frozen=True)
class EnvironmentVariableMissingWarning(FileReadWarning):
    severity = SeverityLevel.HIGH
    variables: frozenset[str]
    identifiers: frozenset[Hashable] | None = None

    def get_message(self) -> str:
        from cognite_toolkit._cdf_tk.utils import humanize_collection

        suffix = "s are" if len(self.variables) > 1 else " is"
        quoted_variables = humanize_collection(
            [f"{variable!r}" for variable in self.variables], sort=True, bind_word="and"
        )
        return f"The environment variable{suffix} missing: {quoted_variables}"


@dataclass(frozen=True)
class ResourceFormatWarning(FileReadWarning):
    severity: ClassVar[SeverityLevel] = SeverityLevel.HIGH
    errors: tuple[str, ...]
    text: str | None = None

    def get_message(self) -> str:
        sep = "\n     * "
        errors = sep.join(map(escape, self.errors))
        s = "s" if len(self.errors) > 1 else ""
        text = self.text or ""
        return f"{type(self).__name__} {text}{len(self.errors)} error{s}:{sep}{errors}"
