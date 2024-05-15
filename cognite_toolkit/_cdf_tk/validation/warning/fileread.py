from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from cognite_toolkit._cdf_tk.user_warnings import ToolkitWarning


@dataclass(frozen=True)
class FileReadWarning(ToolkitWarning, ABC):
    filepath: Path
    id_value: str
    id_name: str


@dataclass(frozen=True)
class UnusedParameter(FileReadWarning):
    actual: str

    def group_key(self) -> tuple[Any, ...]:
        return self.filepath, self.id_value, self.id_name

    def group_header(self) -> str:
        return f"    In File {str(self.filepath)!r}\n    In entry {self.id_name}={self.id_value!r}"

    def __str__(self) -> str:
        return f"{type(self).__name__}: Parameter {self.actual!r} is not used in {self.filepath.name}."


@dataclass(frozen=True)
class SnakeCaseWarning(UnusedParameter):
    expected: str

    def get_message(self) -> str:
        return f"CaseWarning: Got {self.actual!r}. Did you mean {self.expected!r}?"


@dataclass(frozen=True)
class YAMLFileWarning(ToolkitWarning, ABC):
    filepath: Path
    # None is a dictionary, number is a list
    element_no: int | None
    path: tuple[str | int, ...]

    def group_key(self) -> tuple[Any, ...]:
        if self.element_no is None:
            return (self.filepath,)
        else:
            return self.filepath, self.element_no

    def group_header(self) -> str:
        if self.element_no is None:
            return f"    In File {str(self.filepath)!r}"
        else:
            return f"    In File {str(self.filepath)!r}\n    In entry {self.element_no}"

    @property
    def _location(self) -> str:
        if self.element_no is None and not self.path:
            return f"{self.filepath!r}"
        if self.element_no is not None:
            value = f" in entry {self.element_no} "
        else:
            value = ""
        if len(self.path) <= 1:
            return f"{value}"
        else:
            return f"{value} in section {self.path!r}"


@dataclass(frozen=True)
class UnusedParameterWarning(YAMLFileWarning):
    actual: str

    def get_message(self) -> str:
        return f"{type(self).__name__}: Parameter {self.actual!r} is not used{self._location}."


@dataclass(frozen=True)
class UnresolvedVariableWarning(YAMLFileWarning):
    variable: str

    def get_message(self) -> str:
        return f"{type(self).__name__}: Variable {self.variable!r} is not resolved{self._location}."


@dataclass(frozen=True)
class CaseTypoWarning(UnusedParameterWarning):
    expected: str

    def get_message(self) -> str:
        return f"{type(self).__name__}: Got {self.actual!r}. Did you mean {self.expected!r}?{self._location}."


@dataclass(frozen=True)
class MissingRequiredParameter(YAMLFileWarning):
    expected: str

    def get_message(self) -> str:
        return f"{type(self).__name__}: Missing required parameter {self.expected!r}{self._location}."


@dataclass(frozen=True)
class TemplateVariableWarning(FileReadWarning):
    path: str

    def group_key(self) -> tuple[Any, ...]:
        return (self.path,)

    def group_header(self) -> str:
        return f"    In Section {str(self.path)!r}"

    def get_message(self) -> str:
        return f"{type(self).__name__}: Variable {self.id_name!r} has value {self.id_value!r} in file: {self.filepath.name}. Did you forget to change it?"


@dataclass(frozen=True)
class DataSetMissingWarning(FileReadWarning):
    resource_name: str

    def group_key(self) -> tuple[Any, ...]:
        return (self.filepath,)

    def group_header(self) -> str:
        return f"    In File {str(self.filepath)!r}"

    def get_message(self) -> str:
        # Avoid circular import
        from cognite_toolkit._cdf_tk.load import TransformationLoader

        if self.filepath.parent.name == TransformationLoader.folder_name:
            return f"{type(self).__name__}: It is recommended to use a data set if source or destination can be scoped with a data set. If not, ignore this warning."
        else:
            return f"{type(self).__name__}: It is recommended that you set dataSetExternalId for {self.resource_name}. This is missing in {self.filepath.name}. Did you forget to add it?"
