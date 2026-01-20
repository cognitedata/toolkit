from typing import Any, Literal

from pydantic import BaseModel, Field, field_serializer
from pydantic.alias_generators import to_camel

from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import NodeReference
from cognite_toolkit._cdf_tk.client.resource_classes.legacy.migration import AssetCentricId
from cognite_toolkit._cdf_tk.storageio.logger import LogEntry


class MigrationIssue(LogEntry):
    """Represents an issue encountered during migration."""

    type: str

    @property
    def has_issues(self) -> bool:
        """Check if there are any issues recorded in this MigrationIssue."""
        return True

    def dump(self) -> dict[str, Any]:
        """Serialize the MigrationIssue to a dictionary."""
        return self.model_dump(by_alias=True)


class ThreeDModelMigrationIssue(MigrationIssue):
    """Represents a 3D model migration issue encountered during migration.

    Attributes:
        model_external_id (str): The external ID of the 3D model that could not be migrated.
    """

    type: Literal["threeDModelMigration"] = "threeDModelMigration"
    model_name: str
    model_id: int
    error_message: list[str] = Field(default_factory=list)

    @property
    def has_issues(self) -> bool:
        """Check if there are any issues recorded in this ThreeDModelMigrationIssue."""
        return bool(self.error_message)


class ChartMigrationIssue(MigrationIssue):
    """Represents a chart migration issue encountered during migration.

    Attributes:
        chart_external_id (str): The external ID of the chart that could not be migrated.
    """

    type: Literal["chartMigration"] = "chartMigration"
    chart_external_id: str
    missing_timeseries_ids: list[int] = Field(default_factory=list)
    missing_timeseries_external_ids: list[str] = Field(default_factory=list)
    missing_timeseries_identifier: list[str] = Field(default_factory=list)

    @property
    def has_issues(self) -> bool:
        """Check if there are any issues recorded in this ChartMigrationIssue."""
        return bool(
            self.missing_timeseries_ids or self.missing_timeseries_external_ids or self.missing_timeseries_identifier
        )


class CanvasMigrationIssue(MigrationIssue):
    type: Literal["canvasMigration"] = "canvasMigration"
    canvas_external_id: str
    canvas_name: str
    missing_reference_ids: list[AssetCentricId] = Field(default_factory=list)

    @property
    def has_issues(self) -> bool:
        """Check if there are any issues recorded in this CanvasMigrationIssue."""
        return bool(self.missing_reference_ids)


class ReadIssue(MigrationIssue):
    """Represents a read issue encountered during migration."""

    ...


class ReadFileIssue(ReadIssue):
    """Represents a read issue encountered during migration of a file.

    Attributes:
        row_no (int): The row number in the CSV file where the issue occurred.
        error (str | None): An optional error message providing additional details about the read issue.
    """

    type: Literal["fileRead"] = "fileRead"

    row_no: int
    error: str | None = None


class ReadAPIIssue(ReadIssue):
    """Represents a read issue encountered during migration from the API.

    Attributes:
        asset_centric_id (AssetCentricId): The identifier of the asset-centric resource that could not be read.
        error (str | None): An optional error message providing additional details about the read issue.
    """

    type: Literal["apiRead"] = "apiRead"
    asset_centric_id: AssetCentricId
    error: str | None = None

    @field_serializer("asset_centric_id")
    def serialize_asset_centric_id(self, asset_centric_id: AssetCentricId) -> dict[str, Any]:
        return {
            "resourceType": asset_centric_id.resource_type,
            "id": asset_centric_id.id_,
        }


class FailedConversion(BaseModel, alias_generator=to_camel, extra="ignore", populate_by_name=True):
    """Represents a property that failed to convert during migration.

    Attributes:
        value (str | int | float | bool | None): The value that failed to convert
        error (str): The error message explaining why the conversion failed.

    """

    property_id: str
    value: str | int | float | bool | None | list | dict
    error: str


class InvalidPropertyDataType(BaseModel, alias_generator=to_camel, extra="ignore", populate_by_name=True):
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

    type: Literal["conversion"] = "conversion"
    asset_centric_id: AssetCentricId
    instance_id: NodeReference
    missing_asset_centric_properties: list[str] = Field(default_factory=list)
    missing_instance_properties: list[str] = Field(default_factory=list)
    invalid_instance_property_types: list[InvalidPropertyDataType] = Field(default_factory=list)
    failed_conversions: list[FailedConversion] = Field(default_factory=list)
    ignored_asset_centric_properties: list[str] = Field(default_factory=list)
    missing_instance_space: str | None = None

    @property
    def has_issues(self) -> bool:
        """Check if there are any issues recorded in this ConversionIssue."""
        return bool(
            self.missing_asset_centric_properties
            or self.missing_instance_properties
            or self.invalid_instance_property_types
            or self.failed_conversions
            or self.missing_instance_space
        )

    @field_serializer("asset_centric_id")
    def serialize_asset_centric_id(self, asset_centric_id: AssetCentricId) -> dict[str, Any]:
        return {
            "resourceType": asset_centric_id.resource_type,
            "id": asset_centric_id.id_,
        }


class WriteIssue(MigrationIssue):
    """Represents a write issue encountered during migration.

    Attributes:
        status_code (int): The HTTP status code returned during the write operation.
        message (str | None): An optional message providing additional details about the write issue.
    """

    type: Literal["write"] = "write"
    status_code: int
    message: str | None = None
