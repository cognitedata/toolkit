from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Hashable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar

from .base import SeverityLevel, ToolkitWarning


@dataclass(frozen=True)
class FileReadWarning(ToolkitWarning, ABC):
    severity: ClassVar[SeverityLevel]
    filepath: Path

    def group_key(self) -> tuple[Any, ...]:
        return (self.filepath,)

    def group_header(self) -> str:
        return f"    In File {str(self.filepath)!r}"

    def __str__(self) -> str:
        return self.get_message()


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
class DuplicatedItemWarning(YAMLFileWarning):
    severity = SeverityLevel.MEDIUM
    identifier: Hashable
    first_location: Path

    def get_message(self) -> str:
        return (
            f"{type(self).__name__}: Duplicated item with identifier "
            f"{self.identifier!r} first seen in {self.first_location.name}."
        )


@dataclass(frozen=True)
class UnknownResourceTypeWarning(YAMLFileWarning):
    severity = SeverityLevel.MEDIUM

    suggestion: str | None

    def get_message(self) -> str:
        msg = f"{type(self).__name__}: In file {self.filepath.as_posix()!r}."
        if self.suggestion:
            msg += f" Did you mean to call the file {self.suggestion!r}?"
        return msg


@dataclass(frozen=True)
class UnusedParameterWarning(YAMLFileWithElementWarning):
    severity = SeverityLevel.LOW
    actual: str

    def get_message(self) -> str:
        return f"{type(self).__name__}: Parameter {self.actual!r} is not used{self._location}."


@dataclass(frozen=True)
class UnresolvedVariableWarning(FileReadWarning):
    severity = SeverityLevel.HIGH
    variable: str

    def get_message(self) -> str:
        return f"{type(self).__name__}: Variable {self.variable!r} is not resolved in {self.filepath}."


@dataclass(frozen=True)
class ResourceMissingIdentifierWarning(YAMLFileWarning):
    severity: ClassVar[SeverityLevel] = SeverityLevel.HIGH
    message: ClassVar[str] = "The resource is missing an identifier:"
    resource: str
    ext_id_type: str

    def get_message(self) -> str:
        message = f"The {self.resource} {self.filepath} is missing the {self.ext_id_type} field(s)."
        return message


@dataclass(frozen=True)
class NamingConventionWarning(YAMLFileWarning):
    severity: ClassVar[SeverityLevel] = SeverityLevel.LOW
    message: ClassVar[str] = "The naming convention is not followed:"
    resource: str
    ext_id_type: str
    external_id: str

    @property
    @abstractmethod
    def recommendation(self) -> str:
        raise NotImplementedError()

    def get_message(self) -> str:
        message = (
            f"The {self.ext_id_type} identifier [bold]{self.external_id!r}[/bold] of the resource {self.resource} "
            f"does not follow the recommended naming convention {self.recommendation}"
        )
        return message


@dataclass(frozen=True)
class PrefixConventionWarning(NamingConventionWarning):
    prefix: str

    @property
    def recommendation(self) -> str:
        return f"of prefixing with {self.prefix!r}."


@dataclass(frozen=True)
class NamespacingConventionWarning(NamingConventionWarning):
    namespace: str

    @property
    def recommendation(self) -> str:
        return f"of using {self.namespace!r} as separator."


@dataclass(frozen=True)
class CaseTypoWarning(UnusedParameterWarning):
    expected: str

    def get_message(self) -> str:
        return f"{type(self).__name__}: Got {self.actual!r}. Did you mean {self.expected!r}?{self._location}."


@dataclass(frozen=True)
class MissingRequiredParameterWarning(YAMLFileWithElementWarning):
    severity: ClassVar[SeverityLevel] = SeverityLevel.HIGH
    expected: str

    def get_message(self) -> str:
        return f"{type(self).__name__}: Missing required parameter {self.expected!r}{self._location}."


@dataclass(frozen=True)
class MissingRequiredIdentifierWarning(YAMLFileWithElementWarning):
    expected: tuple[str, ...]

    def get_message(self) -> str:
        return f"{type(self).__name__}: Missing required identifier {self.expected!r}{self._location}."


@dataclass(frozen=True)
class TemplateVariableWarning(IdentifiedResourceFileReadWarning):
    path: str

    def get_message(self) -> str:
        return f"{type(self).__name__}: Variable {self.id_name!r} has value {self.id_value!r} in file: {self.filepath.name}. Did you forget to change it?"


@dataclass(frozen=True)
class DataSetMissingWarning(IdentifiedResourceFileReadWarning):
    severity = SeverityLevel.MEDIUM
    resource_name: str

    def get_message(self) -> str:
        # Avoid circular import
        from cognite_toolkit._cdf_tk.loaders import TransformationLoader

        if self.filepath.parent.name == TransformationLoader.folder_name:
            return f"{type(self).__name__}: It is recommended to use a data set if source or destination can be scoped with a data set. If not, ignore this warning."
        else:
            return f"{type(self).__name__}: It is recommended that you set dataSetExternalId for {self.resource_name}. This is missing in {self.filepath.name}. Did you forget to add it?"


@dataclass(frozen=True)
class SourceFileModifiedWarning(FileReadWarning):
    severity = SeverityLevel.ERROR

    def get_message(self) -> str:
        message = (
            f"{type(self).__name__}: The source file {self.filepath} has been modified since the last build. "
            "Please rebuild the project."
        )
        return message


@dataclass(frozen=True)
class MissingFileWarning(FileReadWarning):
    severity = SeverityLevel.MEDIUM
    attempted_check: str

    def get_message(self) -> str:
        message = f"{type(self).__name__}: The file {self.filepath} is missing. Cannot verify {self.attempted_check}."
        return message
