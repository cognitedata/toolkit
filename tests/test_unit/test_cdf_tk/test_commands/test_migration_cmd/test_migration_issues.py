from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import NodeId
from cognite_toolkit._cdf_tk.client.resource_classes.migration import (
    AssetCentricId,
)
from cognite_toolkit._cdf_tk.commands._migrate.issues import (
    ConversionIssue,
    FailedConversion,
    InvalidPropertyDataType,
)


class TestMigrationIssues:
    def test_conversion_issue_minimal(self) -> None:
        asset_centric_id = AssetCentricId(resource_type="asset", id_=456)
        instance_id = NodeId(space="test_space", external_id="test_instance")

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
            "noMappableProperties": False,
        }

    def test_conversion_issue_with_all_fields(self) -> None:
        asset_centric_id = AssetCentricId(resource_type="timeseries", id_=789)
        instance_id = NodeId(space="demo_space", external_id="demo_instance")

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
            "noMappableProperties": False,
        }
