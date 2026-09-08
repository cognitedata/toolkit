from dataclasses import dataclass
from typing import ClassVar

from cognite_toolkit._cdf_tk.tk_warnings.base import GeneralWarning, SeverityLevel, ToolkitWarning


@dataclass(frozen=True)
class LowSeverityWarning(GeneralWarning):
    severity: ClassVar[SeverityLevel] = SeverityLevel.LOW
    message_raw: str

    def get_message(self) -> str:
        return self.message_raw


@dataclass(frozen=True)
class MediumSeverityWarning(GeneralWarning):
    severity: ClassVar[SeverityLevel] = SeverityLevel.MEDIUM
    message_raw: str

    def get_message(self) -> str:
        return self.message_raw


@dataclass(frozen=True)
class HighSeverityWarning(GeneralWarning):
    severity: ClassVar[SeverityLevel] = SeverityLevel.HIGH
    message_raw: str

    def get_message(self) -> str:
        return self.message_raw


@dataclass(frozen=True)
class MissingCapabilityWarning(GeneralWarning):
    severity: ClassVar[SeverityLevel] = SeverityLevel.MEDIUM
    message: ClassVar[str] = "Missing capability:"
    capability: str

    def get_message(self) -> str:
        msg = f"The principal lacks the required access capability {self.capability} in the CDF project"
        return msg


@dataclass(frozen=True)
class ToolkitDeprecationWarning(ToolkitWarning, DeprecationWarning):
    severity = SeverityLevel.HIGH
    message: ClassVar[str] = "The '{feature}' is deprecated and will be removed in a future version."

    feature: str
    alternative: str | None = None
    removal_version: str | None = None

    def get_message(self) -> str:
        msg = self.message.format(feature=self.feature)
        if self.alternative:
            msg += f"\nUse {self.alternative!r} instead."
        if self.removal_version:
            msg += f"\nIt will be removed in version {self.removal_version}."

        return msg


@dataclass(frozen=True)
class HTTPWarning(ToolkitWarning):
    severity: ClassVar[SeverityLevel] = SeverityLevel.MEDIUM
    action: str
    message: str
    status_code: int

    def get_message(self) -> str:
        return f"Failed to {self.action}. HTTP status code {self.status_code}: {self.message}"


@dataclass(frozen=True)
class LimitedAccessWarning(ToolkitWarning):
    severity: ClassVar[SeverityLevel] = SeverityLevel.LOW
    message: str

    def get_message(self) -> str:
        return self.message
