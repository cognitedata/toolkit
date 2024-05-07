from __future__ import annotations

from dataclasses import dataclass
from functools import total_ordering
from pathlib import Path
from typing import ClassVar


@dataclass(frozen=True)
class FileReadWarning:
    _type: ClassVar[str]
    filepath: Path
    id_value: str
    id_name: str


@total_ordering
@dataclass(frozen=True)
class SnakeCaseWarning(FileReadWarning):
    actual: str
    expected: str

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, SnakeCaseWarning):
            return NotImplemented
        return (self.filepath, self.id_value, self.expected, self.actual) < (
            other.filepath,
            other.id_value,
            other.expected,
            other.actual,
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SnakeCaseWarning):
            return NotImplemented
        return (self.filepath, self.id_value, self.expected, self.actual) == (
            other.filepath,
            other.id_value,
            other.expected,
            other.actual,
        )

    def __str__(self) -> str:
        return f"CaseWarning: Got {self.actual!r}. Did you mean {self.expected!r}?"


@total_ordering
@dataclass(frozen=True)
class TemplateVariableWarning(FileReadWarning):
    path: str

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, TemplateVariableWarning):
            return NotImplemented
        return (self.id_name, self.id_value, self.path) < (other.id_name, other.id_value, other.path)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TemplateVariableWarning):
            return NotImplemented
        return (self.id_name, self.id_value, self.path) == (other.id_name, other.id_value, other.path)

    def __str__(self) -> str:
        return f"{type(self).__name__}: Variable {self.id_name!r} has value {self.id_value!r} in file: {self.filepath.name}. Did you forget to change it?"


@total_ordering
@dataclass(frozen=True)
class DataSetMissingWarning(FileReadWarning):
    resource_name: str

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, DataSetMissingWarning):
            return NotImplemented
        return (self.id_name, self.id_value, self.filepath) < (other.id_name, other.id_value, other.filepath)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DataSetMissingWarning):
            return NotImplemented
        return (self.id_name, self.id_value, self.filepath) == (other.id_name, other.id_value, other.filepath)

    def __str__(self) -> str:
        # Avoid circular import
        from cognite_toolkit._cdf_tk.load import TransformationLoader

        if self.filepath.parent.name == TransformationLoader.folder_name:
            return f"{type(self).__name__}: It is recommended to use a data set if source or destination can be scoped with a data set. If not, ignore this warning."
        else:
            return f"{type(self).__name__}: It is recommended that you set dataSetExternalId for {self.resource_name}. This is missing in {self.filepath.name}. Did you forget to add it?"
