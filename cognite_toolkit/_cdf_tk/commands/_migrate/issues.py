import json
from typing import ClassVar

from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.utils._text import to_camel_case
from pydantic import BaseModel, Field

from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal


class MigrationObject(BaseModel, alias_generator=to_camel_case): ...


class MigrationIssue(MigrationObject):
    """Represents an issue encountered during migration."""

    type: ClassVar[str]

    def dump(self) -> dict[str, JsonVal]:
        # Dump json to ensure it is serializable
        dumped = json.loads(self.model_dump_json(by_alias=True))
        dumped["type"] = self.type
        return dumped


class ReadIssue(MigrationIssue):
    """Represents a read issue encountered during migration."""

    type: ClassVar[str] = "read"


class ReadFileIssue(ReadIssue):
    """Represents a read issue encountered during migration of a file.

    Attributes:
        row_no (int): The row number in the CSV file where the issue occurred.
        error (str | None): An optional error message providing additional details about the read issue.
    """

    row_no: int
    error: str | None = None


class FailedConversion(MigrationObject):
    """Represents a property that failed to convert during migration.

    Attributes:
        value (str | int | float | bool | None): The value that failed to convert
        error (str): The error message explaining why the conversion failed.

    """

    value: JsonVal
    error: str


class InvalidProperty(MigrationObject):
    """Represents a property with an invalid type during migration.

    Attributes:
        property_id (str): The identifier of the property.
        expected_type (str): The expected type of the property.
    """

    property_id: str
    expected_type: str


class ConversionIssue(MigrationIssue):
    """Represents a conversion issue encountered during migration.

    Attributes:
        id (int): The identifier of the asset-centric resource.
        instance_id (NodeId): The NodeId of the data model instance.
        missing_source_properties (list[str]): List of source properties that are missing.
        missing_target_properties (list[str]): List of target properties that are missing.
        invalid_target_property_types (list[InvalidProperty]): List of properties with invalid types.
        failed_conversions (list[FailedConversion]): List of properties that failed to convert with reasons.
    """

    type: ClassVar[str] = "conversion"
    id: int
    instance_id: NodeId
    missing_source_properties: list[str] = Field(default_factory=list)
    missing_target_properties: list[str] = Field(default_factory=list)
    invalid_target_property_types: list[InvalidProperty] = Field(default_factory=list)
    failed_conversions: list[FailedConversion] = Field(default_factory=list)


class WriteIssue(MigrationIssue):
    """Represents a write issue encountered during migration.

    Attributes:
        status_code (int): The HTTP status code returned during the write operation.
        message (str | None): An optional message providing additional details about the write issue.
    """

    type: ClassVar[str] = "write"
    status_code: int
    message: str | None = None
