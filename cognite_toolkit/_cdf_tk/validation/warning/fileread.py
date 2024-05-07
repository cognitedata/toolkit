from __future__ import annotations

import itertools
from collections import UserList
from collections.abc import Collection
from dataclasses import dataclass
from functools import total_ordering
from pathlib import Path
from typing import ClassVar, Generic, TypeVar


@dataclass(frozen=True)
class LoadWarning:
    _type: ClassVar[str]
    filepath: Path
    id_value: str
    id_name: str


@total_ordering
@dataclass(frozen=True)
class SnakeCaseWarning(LoadWarning):
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
class TemplateVariableWarning(LoadWarning):
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
class DataSetMissingWarning(LoadWarning):
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


T_Warning = TypeVar("T_Warning", bound=LoadWarning)


class Warnings(UserList, Generic[T_Warning]):
    def __init__(self, collection: Collection[T_Warning] | None = None):
        super().__init__(collection or [])


class SnakeCaseWarningList(Warnings[SnakeCaseWarning]):
    def __str__(self) -> str:
        output = [""]
        for (file, identifier, id_name), file_warnings in itertools.groupby(
            sorted(self), key=lambda w: (w.filepath, w.id_value, w.id_name)
        ):
            output.append(f"    In File {str(file)!r}")
            output.append(f"    In entry {id_name}={identifier!r}")
            for warning in file_warnings:
                output.append(f"{'    ' * 2}{warning!s}")

        return "\n".join(output)


class TemplateVariableWarningList(Warnings[TemplateVariableWarning]):
    def __str__(self) -> str:
        output = [""]
        for path, module_warnings in itertools.groupby(sorted(self), key=lambda w: w.path):
            if path:
                output.append(f"    In Section {str(path)!r}")
            for warning in module_warnings:
                output.append(f"{'    ' * 2}{warning!s}")

        return "\n".join(output)


class DataSetMissingWarningList(Warnings[DataSetMissingWarning]):
    def __str__(self) -> str:
        output = [""]
        for filepath, warnings in itertools.groupby(sorted(self), key=lambda w: w.filepath):
            output.append(f"    In file {str(filepath)!r}")
            for warning in warnings:
                output.append(f"{'    ' * 2}{warning!s}")

        return "\n".join(output)
