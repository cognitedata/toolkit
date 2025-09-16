import json
from typing import Any, ClassVar

from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.utils._identifier import InstanceId
from cognite.client.utils._text import to_camel_case
from pydantic import BaseModel, Field, field_serializer

from cognite_toolkit._cdf_tk.client.data_classes.migration import AssetCentricId
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal


class MigrationObject(BaseModel, alias_generator=to_camel_case, extra="ignore", populate_by_name=True): ...


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

    type: ClassVar[str] = "fileRead"

    row_no: int
    error: str | None = None


class ReadAPIIssue(ReadIssue):
    """Represents a read issue encountered during migration from the API.

    Attributes:
        asset_centric_id (AssetCentricId): The identifier of the asset-centric resource that could not be read.
        error (str | None): An optional error message providing additional details about the read issue.
    """

    type: ClassVar[str] = "apiRead"
    asset_centric_id: AssetCentricId
    error: str | None = None

    @field_serializer("asset_centric_id")
    def serialize_asset_centric_id(self, asset_centric_id: AssetCentricId) -> dict[str, Any]:
        return {
            "resourceType": asset_centric_id.resource_type,
            "id": asset_centric_id.id_,
        }


class FailedConversion(MigrationObject):
    """Represents a property that failed to convert during migration.

    Attributes:
        value (str | int | float | bool | None): The value that failed to convert
        error (str): The error message explaining why the conversion failed.

    """

    property_id: str
    value: str | int | float | bool | None | list | dict
    error: str


class InvalidPropertyDataType(MigrationObject):
    """Represents a property with an invalid type during migration.

    Attributes:
        property_id (str): The identifier of the property in asset-centric.

        expected_type (str): The expected type of the property.
    """

    property_id: str
    expected_type: str


class ConversionIssue(MigrationIssue):
    """Represents a conversion issue encountered during migration.

    Attributes:
        asset_centric_id (AssetCentricId): The identifier of the asset-centric resource.
        instance_id (InstanceId): The NodeId of the data model instance.
        missing_asset_centric_properties (list[str]): List of source properties that are missing.
        missing_instance_properties (list[str]): List of target properties that are missing.
        invalid_instance_property_types (list[InvalidPropertyDataType]): List of properties with invalid types.
        failed_conversions (list[FailedConversion]): List of properties that failed to convert with reasons.
    """

    type: ClassVar[str] = "conversion"
    asset_centric_id: AssetCentricId
    instance_id: InstanceId
    missing_asset_centric_properties: list[str] = Field(default_factory=list)
    missing_instance_properties: list[str] = Field(default_factory=list)
    invalid_instance_property_types: list[InvalidPropertyDataType] = Field(default_factory=list)
    failed_conversions: list[FailedConversion] = Field(default_factory=list)
    ignored_asset_centric_properties: list[str] = Field(default_factory=list)

    @property
    def has_issues(self) -> bool:
        """Check if there are any issues recorded in this ConversionIssue."""
        return bool(
            self.missing_asset_centric_properties
            or self.missing_instance_properties
            or self.invalid_instance_property_types
            or self.failed_conversions
        )

    @field_serializer("instance_id")
    def serialize_instance_id(self, instance_id: NodeId) -> dict[str, str]:
        return instance_id.dump(include_instance_type=True)

    @field_serializer("asset_centric_id")
    def serialize_asset_centric_id(self, asset_centric_id: AssetCentricId) -> dict[str, Any]:
        return {
            "resourceType": asset_centric_id.resource_type,
            "id": asset_centric_id.id_,
        }


class WriteIssue(MigrationIssue):
    """Represents a write issue encountered during migration.

    Attributes:
        instance_id (InstanceId): The InstanceId of the data model instance that could not be written.
        status_code (int): The HTTP status code returned during the write operation.
        message (str | None): An optional message providing additional details about the write issue.
    """

    type: ClassVar[str] = "write"
    instance_id: InstanceId
    status_code: int
    message: str | None = None

    @field_serializer("instance_id")
    def serialize_instance_id(self, instance_id: NodeId) -> dict[str, str]:
        return instance_id.dump(include_instance_type=True)
