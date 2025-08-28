from typing import ClassVar

from cognite.client.data_classes.data_modeling import NodeId

from cognite_toolkit._cdf_tk.client.data_classes.migration import AssetCentricId
from cognite_toolkit._cdf_tk.commands._migrate.issues import (
    ConversionIssue,
    FailedConversion,
    InvalidPropertyDataType,
    ReadAPIIssue,
    ReadFileIssue,
    ReadIssue,
    WriteIssue,
)


class TestMigrationIssues:
    def test_read_file_issue(self) -> None:
        issue = ReadFileIssue(row_no=10, error="Cannot read column 'id' value is not an integer")
        assert issue.dump() == {
            "type": "fileRead",
            "rowNo": 10,
            "error": "Cannot read column 'id' value is not an integer",
        }

    def test_read_api_issue(self) -> None:
        asset_centric_id = AssetCentricId(resource_type="asset", id_=123)
        issue = ReadAPIIssue(asset_centric_id=asset_centric_id, error="API error")
        assert issue.dump() == {
            "type": "apiRead",
            "assetCentricId": {"resourceType": "asset", "id": 123},
            "error": "API error",
        }

    def test_read_issue_subclass(self) -> None:
        class CustomReadIssue(ReadIssue):
            type: ClassVar[str] = "customRead"
            custom_field: str

        issue = CustomReadIssue(custom_field="custom value")
        assert issue.dump() == {
            "type": "customRead",
            "customField": "custom value",
        }

    def test_conversion_issue_minimal(self) -> None:
        asset_centric_id = AssetCentricId(resource_type="asset", id_=456)
        instance_id = NodeId(space="test_space", external_id="test_instance")

        conversion_issue = ConversionIssue(asset_centric_id=asset_centric_id, instance_id=instance_id)

        assert conversion_issue.dump() == {
            "type": "conversion",
            "assetCentricId": {"resourceType": "asset", "id": 456},
            "instanceId": {"space": "test_space", "externalId": "test_instance", "type": "node"},
            "failedConversions": [],
            "invalidInstancePropertyTypes": [],
            "missingAssetCentricProperties": [],
            "missingInstanceProperties": [],
        }

    def test_conversion_issue_with_all_fields(self) -> None:
        asset_centric_id = AssetCentricId(resource_type="timeseries", id_=789)
        instance_id = NodeId(space="demo_space", external_id="demo_instance")

        failed_conversion = FailedConversion(property_id="value", value="not_a_number", error="Cannot convert to float")
        invalid_property = InvalidPropertyDataType(property_id="status", expected_type="boolean")

        conversion_issue = ConversionIssue(
            asset_centric_id=asset_centric_id,
            instance_id=instance_id,
            missing_asset_centric_properties=["missing_source_prop"],
            missing_instance_properties=["missing_target_prop"],
            invalid_instance_property_types=[invalid_property],
            failed_conversions=[failed_conversion],
        )

        assert conversion_issue.dump() == {
            "type": "conversion",
            "assetCentricId": {"resourceType": "timeseries", "id": 789},
            "instanceId": {"space": "demo_space", "externalId": "demo_instance", "type": "node"},
            "missingAssetCentricProperties": ["missing_source_prop"],
            "missingInstanceProperties": ["missing_target_prop"],
            "invalidInstancePropertyTypes": [{"propertyId": "status", "expectedType": "boolean"}],
            "failedConversions": [{"propertyId": "value", "value": "not_a_number", "error": "Cannot convert to float"}],
        }

    def test_write_issue_minimal(self) -> None:
        instance_id = NodeId(space="write_space", external_id="write_instance")

        write_issue = WriteIssue(instance_id=instance_id, status_code=400)

        assert write_issue.dump() == {
            "type": "write",
            "instanceId": {"space": "write_space", "externalId": "write_instance", "type": "node"},
            "statusCode": 400,
            "message": None,
        }

    def test_write_issue_with_message(self) -> None:
        instance_id = NodeId(space="error_space", external_id="error_instance")

        write_issue = WriteIssue(instance_id=instance_id, status_code=500, message="Internal server error occurred")

        assert write_issue.dump() == {
            "type": "write",
            "instanceId": {"space": "error_space", "externalId": "error_instance", "type": "node"},
            "statusCode": 500,
            "message": "Internal server error occurred",
        }
