from typing import Literal

from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import NodeReference
from cognite_toolkit._cdf_tk.client.resource_classes.legacy.migration import (
    AssetCentricId,
)
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
        issue = ReadFileIssue(id="issue-1", row_no=10, error="Cannot read column 'id' value is not an integer")
        assert issue.dump() == {
            "id": "issue-1",
            "type": "fileRead",
            "rowNo": 10,
            "error": "Cannot read column 'id' value is not an integer",
        }

    def test_read_api_issue(self) -> None:
        asset_centric_id = AssetCentricId(resource_type="asset", id_=123)
        issue = ReadAPIIssue(id="issue-2", asset_centric_id=asset_centric_id, error="API error")
        assert issue.dump() == {
            "id": "issue-2",
            "type": "apiRead",
            "assetCentricId": {"resourceType": "asset", "id": 123},
            "error": "API error",
        }

    def test_read_issue_subclass(self) -> None:
        class CustomReadIssue(ReadIssue):
            type: Literal["customRead"] = "customRead"
            custom_field: str

        issue = CustomReadIssue(id="issue-3", custom_field="custom value")
        assert issue.dump() == {
            "id": "issue-3",
            "type": "customRead",
            "customField": "custom value",
        }

    def test_conversion_issue_minimal(self) -> None:
        asset_centric_id = AssetCentricId(resource_type="asset", id_=456)
        instance_id = NodeReference(space="test_space", external_id="test_instance")

        conversion_issue = ConversionIssue(id="issue-4", asset_centric_id=asset_centric_id, instance_id=instance_id)

        assert conversion_issue.dump() == {
            "id": "issue-4",
            "type": "conversion",
            "assetCentricId": {"resourceType": "asset", "id": 456},
            "instanceId": {"space": "test_space", "externalId": "test_instance"},
            "failedConversions": [],
            "invalidInstancePropertyTypes": [],
            "missingAssetCentricProperties": [],
            "missingInstanceProperties": [],
            "ignoredAssetCentricProperties": [],
            "missingInstanceSpace": None,
        }

    def test_conversion_issue_with_all_fields(self) -> None:
        asset_centric_id = AssetCentricId(resource_type="timeseries", id_=789)
        instance_id = NodeReference(space="demo_space", external_id="demo_instance")

        failed_conversion = FailedConversion(property_id="value", value="not_a_number", error="Cannot convert to float")
        invalid_property = InvalidPropertyDataType(property_id="status", expected_type="boolean")

        conversion_issue = ConversionIssue(
            id="issue-5",
            asset_centric_id=asset_centric_id,
            instance_id=instance_id,
            missing_asset_centric_properties=["missing_source_prop"],
            missing_instance_properties=["missing_target_prop"],
            invalid_instance_property_types=[invalid_property],
            failed_conversions=[failed_conversion],
        )

        assert conversion_issue.dump() == {
            "id": "issue-5",
            "type": "conversion",
            "assetCentricId": {"resourceType": "timeseries", "id": 789},
            "instanceId": {"space": "demo_space", "externalId": "demo_instance"},
            "missingAssetCentricProperties": ["missing_source_prop"],
            "missingInstanceProperties": ["missing_target_prop"],
            "invalidInstancePropertyTypes": [{"propertyId": "status", "expectedType": "boolean"}],
            "failedConversions": [{"propertyId": "value", "value": "not_a_number", "error": "Cannot convert to float"}],
            "ignoredAssetCentricProperties": [],
            "missingInstanceSpace": None,
        }

    def test_write_issue_minimal(self) -> None:
        write_issue = WriteIssue(id="issue-6", status_code=400)

        assert write_issue.dump() == {
            "id": "issue-6",
            "type": "write",
            "statusCode": 400,
            "message": None,
        }

    def test_write_issue_with_message(self) -> None:
        write_issue = WriteIssue(id="issue-7", status_code=500, message="Internal server error occurred")

        assert write_issue.dump() == {
            "id": "issue-7",
            "type": "write",
            "statusCode": 500,
            "message": "Internal server error occurred",
        }
