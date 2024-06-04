from __future__ import annotations

from collections.abc import Hashable
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar, Union

from cognite_toolkit._cdf_tk.tk_warnings.base import GeneralWarning, SeverityFormat, SeverityLevel, ToolkitWarning


@dataclass(frozen=True)
class UnexpectedFileLocationWarning(ToolkitWarning):
    severity: ClassVar[SeverityLevel] = SeverityLevel.LOW
    filepath: str
    alternative: str

    def get_message(self) -> str:
        message = f"{self.filepath!r} does not exist. Using {self.alternative!r} instead."
        return SeverityFormat.get_rich_severity_format(self.severity, message)


@dataclass(frozen=True)
class ToolkitBugWarning(ToolkitWarning):
    severity: ClassVar[SeverityLevel] = SeverityLevel.HIGH
    message: ClassVar[str] = "Please contact the toolkit maintainers with the error message and traceback:"
    header: str
    traceback: str

    def get_message(self) -> str:
        return SeverityFormat.get_rich_severity_format(self.severity, self.header, self.message, self.traceback)


@dataclass(frozen=True)
class IncorrectResourceWarning(ToolkitWarning):
    severity: ClassVar[SeverityLevel] = SeverityLevel.LOW
    message: ClassVar[str] = "The resource not semantically correct:"
    location: str
    resource: str
    details: str | list[str] | None = None

    def get_message(self) -> str:
        extra_details = []
        if self.details:
            if isinstance(self.details, str):
                extra_details.append(self.details)
            else:
                extra_details.extend(self.details)
        return SeverityFormat.get_rich_severity_format(
            self.severity, self.location, self.message, self.resource, *extra_details
        )


@dataclass(frozen=True)
class LowSeverityWarning(GeneralWarning):
    severity: ClassVar[SeverityLevel] = SeverityLevel.LOW
    message_raw: str

    def get_message(self) -> str:
        return SeverityFormat.get_rich_severity_format(self.severity, self.message_raw)


@dataclass(frozen=True)
class MediumSeverityWarning(GeneralWarning):
    severity: ClassVar[SeverityLevel] = SeverityLevel.MEDIUM
    message_raw: str

    def get_message(self) -> str:
        return SeverityFormat.get_rich_severity_format(self.severity, self.message_raw)


@dataclass(frozen=True)
class HighSeverityWarning(GeneralWarning):
    severity: ClassVar[SeverityLevel] = SeverityLevel.HIGH
    message_raw: str

    def get_message(self) -> str:
        return SeverityFormat.get_rich_severity_format(self.severity, self.message_raw)


@dataclass(frozen=True)
class ToolkitDependenciesIncludedWarning(GeneralWarning):
    severity: ClassVar[SeverityLevel] = SeverityLevel.LOW
    message: ClassVar[str] = "Operation may fail due to missing dependencies:"
    dependencies: Union[None, str, list[str]]

    def get_message(self) -> str:
        output = [SeverityFormat.get_rich_severity_format(self.severity, self.message)]

        if self.dependencies:
            output[0] += ":"
            if isinstance(self.dependencies, str):
                output.append(SeverityFormat.get_rich_detail_format(self.dependencies))
            else:
                for dependency in self.dependencies:
                    output.append(SeverityFormat.get_rich_detail_format(dependency))
        return "\n".join(output)


@dataclass(frozen=True)
class ToolkitNotSupportedWarning(GeneralWarning):
    severity: ClassVar[SeverityLevel] = SeverityLevel.LOW
    message: ClassVar[str] = "This feature is not supported"
    feature: str

    details: str | list[str] | None = None

    def get_message(self) -> str:
        extra_details = []
        if self.details:
            if isinstance(self.details, str):
                extra_details.append(self.details)
            else:
                extra_details.extend(self.details)
        return SeverityFormat.get_rich_severity_format(self.severity, self.message, self.feature, *extra_details)


@dataclass(frozen=True)
class MissingDependencyWarning(GeneralWarning):
    severity: ClassVar[SeverityLevel] = SeverityLevel.HIGH
    message: ClassVar[str] = ""
    dependency_type: str
    identifier: Hashable
    required_by: set[tuple[Hashable, Path]]

    def get_message(self) -> str:
        msg = f"{self.dependency_type} {self.identifier!r} is missing and is required by:"
        for identifier, path in self.required_by:
            msg += f"\n- {identifier!r} in {path}"
        return SeverityFormat.get_rich_severity_format(self.severity, msg)


@dataclass(frozen=True)
class MissingCapabilityWarning(GeneralWarning):
    severity: ClassVar[SeverityLevel] = SeverityLevel.MEDIUM
    message: ClassVar[str] = "Missing capability:"
    capability: str

    def get_message(self) -> str:
        msg = f"The principal lacks the required access capability {self.capability} in the CDF project"
        return SeverityFormat.get_rich_severity_format(self.severity, msg)
