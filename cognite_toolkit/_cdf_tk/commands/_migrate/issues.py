from typing import Any, ClassVar

from pydantic import BaseModel


class MigrationIssue(BaseModel):
    """Represents an issue encountered during migration."""

    type: ClassVar[str]

    def dump(self) -> dict[str, Any]:
        dumped = self.model_dump(by_alias=True)
        dumped["type"] = self.type
        return dumped


class ReadIssue(MigrationIssue):
    """Represents a read issue encountered during migration."""

    type: ClassVar[str] = "read"


class ConversionIssue(MigrationIssue):
    """Represents a conversion issue encountered during migration."""

    type: ClassVar[str] = "conversion"


class WriteIssue(MigrationIssue):
    """Represents a write issue encountered during migration."""

    type: ClassVar[str] = "write"
