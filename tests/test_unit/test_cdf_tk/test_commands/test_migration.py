import json
from datetime import datetime, timezone
from typing import ClassVar

import pytest
from cognite.client.data_classes import Asset, Event, FileMetadata, Sequence, TimeSeries
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.data_classes.data_modeling import data_types as dt
from cognite.client.data_classes.data_modeling.data_types import DirectRelationReference, EnumValue
from cognite.client.data_classes.data_modeling.ids import ContainerId, ViewId
from cognite.client.data_classes.data_modeling.views import MappedProperty, MultiEdgeConnection, ViewProperty

from cognite_toolkit._cdf_tk.client.data_classes.migration import (
    AssetCentricId,
    ResourceViewMapping,
)
from cognite_toolkit._cdf_tk.commands._migrate.conversion import asset_centric_to_dm
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
            "ignoredAssetCentricProperties": [],
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
            "ignoredAssetCentricProperties": [],
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


class TestAssetCentricConversion:
    INSTANCE_ID = NodeId(space="test_space", external_id="test_instance")
    CONTAINER_ID = ContainerId("test_space", "test_container")
    VIEW_ID = ViewId("test_space", "test_view", "v1")
    INSTANCE_SOURCE_VIEW_ID = ViewId("cognite_migration", "InstanceSource", "v1")

    @pytest.mark.parametrize(
        "resource,view_source,view_properties,expected_properties,expected_issue",
        [
            pytest.param(
                # Simple Asset with basic mapping
                Asset(id=123, external_id="asset_123", name="Test Asset", description="A test asset"),
                ResourceViewMapping(
                    external_id="asset_mapping",
                    version=1,
                    last_updated_time=1000000,
                    created_time=1000000,
                    resource_type="asset",
                    view_id=ViewId("test_space", "test_view", "v1"),
                    property_mapping={"name": "assetName", "description": "assetDescription"},
                ),
                {
                    "assetName": MappedProperty(
                        ContainerId("test_space", "test_container"),
                        "assetName",
                        dt.Text(),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                    ),
                    "assetDescription": MappedProperty(
                        ContainerId("test_space", "test_container"),
                        "assetDescription",
                        dt.Text(),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                    ),
                },
                {"assetName": "Test Asset", "assetDescription": "A test asset"},
                ConversionIssue(
                    asset_centric_id=AssetCentricId("asset", 123),
                    instance_id=INSTANCE_ID,
                    ignored_asset_centric_properties=[],
                ),
                id="simple_asset_mapping",
            ),
            pytest.param(
                TimeSeries(
                    id=456,
                    external_id="ts_456",
                    name="Test TimeSeries",
                    description="A test timeseries",
                    unit="celsius",
                    metadata={"sensor_type": "temperature", "location": "room_1"},
                ),
                ResourceViewMapping(
                    external_id="timeseries_mapping",
                    version=1,
                    last_updated_time=1000000,
                    created_time=1000000,
                    resource_type="timeseries",
                    view_id=ViewId("test_space", "test_view", "v1"),
                    property_mapping={
                        "name": "timeseriesName",
                        "unit": "measurementUnit",
                        "metadata.sensor_type": "sensorType",
                        "metadata.location": "deviceLocation",
                    },
                ),
                {
                    "timeseriesName": MappedProperty(
                        ContainerId("test_space", "test_container"),
                        "timeseriesName",
                        dt.Text(),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                    ),
                    "measurementUnit": MappedProperty(
                        ContainerId("test_space", "test_container"),
                        "measurementUnit",
                        dt.Text(),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                    ),
                    "sensorType": MappedProperty(
                        ContainerId("test_space", "test_container"),
                        "sensorType",
                        dt.Text(),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                    ),
                    "deviceLocation": MappedProperty(
                        ContainerId("test_space", "test_container"),
                        "deviceLocation",
                        dt.Text(),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                    ),
                },
                {
                    "timeseriesName": "Test TimeSeries",
                    "measurementUnit": "celsius",
                    "sensorType": "temperature",
                    "deviceLocation": "room_1",
                },
                ConversionIssue(
                    asset_centric_id=AssetCentricId("timeseries", id_=456),
                    instance_id=INSTANCE_ID,
                    ignored_asset_centric_properties=["description"],
                ),
                id="timeseries_with_metadata",
            ),
            pytest.param(
                Event(
                    id=789,
                    external_id="event_789",
                    start_time=1756359489386,
                    end_time=1756359499880,
                    description="Not mapped",
                    metadata={
                        "operator": "John Doe",
                        "severity": "HIGH",
                        "value": "invalid_int",  # This will cause a conversion issue
                        "aConnectionProp": json.dumps(
                            {"externalId": "op_123", "space": "schema_space", "type": "Operation"}
                        ),
                    },
                ),
                ResourceViewMapping(
                    external_id="incomplete_mapping",
                    version=1,
                    last_updated_time=1000000,
                    created_time=1000000,
                    resource_type="event",
                    view_id=ViewId("test_space", "test_view", "v1"),
                    property_mapping={
                        "missing_prop": "targetProp",
                        "startTime": "eventStart",
                        "endTime": "eventEnd",
                        "metadata.operator": "missingDMProp",
                        "metadata.severity": "eventSeverity",
                        "metadata.value": "eventValue",
                        "metadata.missingMetaProp": "anotherMissingDMProp",
                        "metadata.aConnectionProp": "some_other_event",
                    },
                ),
                {
                    "eventStart": MappedProperty(
                        ContainerId("test_space", "test_container"),
                        "eventStart",
                        dt.Timestamp(),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                    ),
                    "eventEnd": MappedProperty(
                        ContainerId("test_space", "test_container"),
                        "eventEnd",
                        dt.Timestamp(),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                    ),
                    "eventSeverity": MappedProperty(
                        ContainerId("test_space", "test_container"),
                        "eventSeverity",
                        dt.Enum({"high": EnumValue(), "low": EnumValue(), "medium": EnumValue()}),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                    ),
                    "eventValue": MappedProperty(
                        ContainerId("test_space", "test_container"),
                        "eventValue",
                        dt.Int64(),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                    ),
                    "some_other_event": MultiEdgeConnection(
                        type=DirectRelationReference("schema_space", "Operation"),
                        source=ViewId("test_space", "test_view", "v1"),
                        name=None,
                        description=None,
                        edge_source=None,
                        direction="outwards",
                    ),
                },
                {
                    "eventStart": datetime(2025, 8, 28, 5, 38, 9, 386000, tzinfo=timezone.utc),
                    "eventEnd": datetime(2025, 8, 28, 5, 38, 19, 880000, tzinfo=timezone.utc),
                    "eventSeverity": "high",
                },
                ConversionIssue(
                    asset_centric_id=AssetCentricId("event", id_=789),
                    instance_id=INSTANCE_ID,
                    ignored_asset_centric_properties=["description"],
                    missing_asset_centric_properties=["metadata.missingMetaProp", "missing_prop"],
                    missing_instance_properties=["anotherMissingDMProp", "missingDMProp", "targetProp"],
                    invalid_instance_property_types=[
                        InvalidPropertyDataType(property_id="some_other_event", expected_type="MappedProperty")
                    ],
                    failed_conversions=[
                        FailedConversion(
                            property_id="metadata.value",
                            value="invalid_int",
                            error="Cannot convert invalid_int to int64.",
                        )
                    ],
                ),
                id="Event with conversion issues (missing properties)",
            ),
            pytest.param(
                FileMetadata(
                    id=321,
                    external_id="file_321",
                    name="Test File",
                    mime_type="application/octet-stream",
                    metadata={"file_type": "pdf", "confidential": "true"},
                ),
                ResourceViewMapping(
                    external_id="file_mapping",
                    version=1,
                    last_updated_time=1000000,
                    created_time=1000000,
                    resource_type="file",
                    view_id=ViewId("test_space", "test_view", "v1"),
                    property_mapping={},
                ),
                {
                    "fileName": MappedProperty(
                        ContainerId("test_space", "test_container"),
                        "fileName",
                        dt.Text(),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                    ),
                    "fileDescription": MappedProperty(
                        ContainerId("test_space", "test_container"),
                        "fileDescription",
                        dt.Text(),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                    ),
                    "fileType": MappedProperty(
                        ContainerId("test_space", "test_container"),
                        "fileType",
                        dt.Text(),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                    ),
                    "isConfidential": MappedProperty(
                        ContainerId("test_space", "test_container"),
                        "isConfidential",
                        dt.Boolean(),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                    ),
                },
                {},
                ConversionIssue(
                    asset_centric_id=AssetCentricId("file", id_=321),
                    instance_id=INSTANCE_ID,
                    ignored_asset_centric_properties=[
                        "metadata.confidential",
                        "metadata.file_type",
                        "mimeType",
                        "name",
                    ],
                    missing_asset_centric_properties=[],
                    missing_instance_properties=[],
                ),
                id="FileMetadata with no mappings (all ignored)",
            ),
            pytest.param(
                Sequence(
                    id=654,
                    name="Test Sequence",
                    description="A test sequence",
                    metadata=None,
                ),
                ResourceViewMapping(
                    external_id="sequence_mapping",
                    version=1,
                    last_updated_time=1000000,
                    created_time=1000000,
                    resource_type="sequence",
                    view_id=ViewId("test_space", "test_view", "v1"),
                    property_mapping={"name": "sequenceName", "metadata.category": "sequenceCategory"},
                ),
                {
                    "sequenceName": MappedProperty(
                        ContainerId("test_space", "test_container"),
                        "sequenceName",
                        dt.Text(),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                    ),
                    "sequenceCategory": MappedProperty(
                        ContainerId("test_space", "test_container"),
                        "sequenceCategory",
                        dt.Text(),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                    ),
                },
                {"sequenceName": "Test Sequence"},
                ConversionIssue(
                    asset_centric_id=AssetCentricId("sequence", id_=654),
                    instance_id=INSTANCE_ID,
                    ignored_asset_centric_properties=["description"],
                    missing_asset_centric_properties=["metadata.category"],
                    missing_instance_properties=[],
                ),
                id="Sequence with partial mapping",
            ),
            pytest.param(
                Asset(id=999, external_id="asset_999", name=None, description=None),
                ResourceViewMapping(
                    external_id="empty_mapping",
                    version=1,
                    last_updated_time=1000000,
                    created_time=1000000,
                    resource_type="asset",
                    view_id=ViewId("test_space", "test_view", "v1"),
                    property_mapping={"name": "assetName", "description": "assetDescription"},
                ),
                {
                    "assetName": MappedProperty(
                        ContainerId("test_space", "test_container"),
                        "assetName",
                        dt.Text(),
                        nullable=False,
                        immutable=False,
                        auto_increment=False,
                    ),
                    "assetDescription": MappedProperty(
                        ContainerId("test_space", "test_container"),
                        "assetDescription",
                        dt.Text(),
                        nullable=False,
                        immutable=False,
                        auto_increment=False,
                    ),
                },
                {},
                ConversionIssue(
                    asset_centric_id=AssetCentricId("asset", id_=999),
                    instance_id=INSTANCE_ID,
                    ignored_asset_centric_properties=[],
                    # Name and description set to None is the same as missing as we have now way of knowing
                    # whether they were explicitly set to None or just not set at all.
                    missing_asset_centric_properties=["description", "name"],
                    missing_instance_properties=[],
                ),
                id="Asset with non-nullable properties all None",
            ),
            pytest.param(
                Event(id=999, external_id="event_999", type="MyType", metadata={"category": "MyCategory"}),
                ResourceViewMapping(
                    external_id="event_mapping",
                    version=1,
                    last_updated_time=1000000,
                    created_time=1000000,
                    resource_type="event",
                    view_id=ViewId("test_space", "test_view", "v1"),
                    property_mapping={"type": "category", "metadata.category": "category"},
                ),
                {
                    "category": MappedProperty(
                        ContainerId("test_space", "test_container"),
                        "category",
                        dt.Text(),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                    ),
                },
                {
                    "category": "MyType",
                },
                ConversionIssue(
                    asset_centric_id=AssetCentricId("event", id_=999),
                    instance_id=INSTANCE_ID,
                    ignored_asset_centric_properties=["metadata.category"],
                ),
                id="Event with overlapping property and metadata mapping (property takes precedence)",
            ),
        ],
    )
    def test_asset_centric_to_dm(
        self,
        resource: Asset | FileMetadata | Event | TimeSeries | Sequence,
        view_source: ResourceViewMapping,
        view_properties: dict[str, ViewProperty],
        expected_properties: dict[str, str],
        expected_issue: ConversionIssue,
    ) -> None:
        actual, issue = asset_centric_to_dm(resource, self.INSTANCE_ID, view_source, view_properties)

        # Check the structure of the returned NodeApply
        assert actual.space == self.INSTANCE_ID.space
        assert actual.external_id == self.INSTANCE_ID.external_id
        assert 1 <= len(actual.sources) <= 2

        # Check the main view source
        if len(actual.sources) == 2:
            main_source = actual.sources[0]
            assert main_source.source == view_source.view_id
            assert main_source.properties == expected_properties
        else:
            assert expected_properties == {}

        # Check the instance source view
        instance_source = actual.sources[-1]
        assert instance_source.source == self.INSTANCE_SOURCE_VIEW_ID
        assert instance_source.properties["resourceType"] == view_source.resource_type
        assert instance_source.properties["id"] == resource.id
        if hasattr(resource, "data_set_id") and resource.data_set_id is not None:
            assert instance_source.properties["dataSetId"] == resource.data_set_id
        if hasattr(resource, "external_id") and resource.external_id is not None:
            assert instance_source.properties["classicExternalId"] == resource.external_id

        assert expected_issue.dump() == issue.dump()
