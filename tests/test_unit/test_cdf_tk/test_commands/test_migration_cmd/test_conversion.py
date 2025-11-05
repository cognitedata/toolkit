import json
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any, ClassVar

import pytest
from cognite.client.data_classes import Annotation, Asset, Event, FileMetadata, Sequence, TimeSeries
from cognite.client.data_classes.data_modeling import (
    EdgeApply,
    NodeId,
    NodeOrEdgeData,
)
from cognite.client.data_classes.data_modeling import data_types as dt
from cognite.client.data_classes.data_modeling.data_types import DirectRelationReference, EnumValue
from cognite.client.data_classes.data_modeling.ids import ContainerId, EdgeId, ViewId
from cognite.client.data_classes.data_modeling.instances import PropertyValueWrite
from cognite.client.data_classes.data_modeling.views import MappedProperty, MultiEdgeConnection, ViewProperty

from cognite_toolkit._cdf_tk.client.data_classes.migration import AssetCentricId, ResourceViewMapping
from cognite_toolkit._cdf_tk.commands._migrate.conversion import (
    DirectRelationCache,
    asset_centric_to_dm,
    create_properties,
)
from cognite_toolkit._cdf_tk.commands._migrate.issues import (
    ConversionIssue,
    FailedConversion,
    InvalidPropertyDataType,
)


class TestCreateProperties:
    INSTANCE_ID = NodeId(space="test_space", external_id="test_instance")
    CONTAINER_ID = ContainerId("test_space", "test_container")
    DEFAULT_CONTAINER_ARGS: ClassVar = dict(nullable=True, immutable=False, auto_increment=False)
    ASSET_CENTRIC_ID = AssetCentricId(resource_type="asset", id_=123)
    EVENT_CENTRIC_ID = AssetCentricId(resource_type="event", id_=456)
    DIRECT_RELATION_CACHE = DirectRelationCache(
        asset={1: DirectRelationReference("instance_space", "MyFirstAsset")},
        source={"sourceA": DirectRelationReference("instance_space", "TheSourceA")},
        file={},
    )

    @pytest.mark.parametrize(
        "dumped,view_properties,property_mapping,expected_properties,expected_issue",
        [
            pytest.param(
                {"name": "MyAsset", "description": "An asset,", "metadata": {"categoryNo": "1"}},
                {
                    "nameId": MappedProperty(CONTAINER_ID, "nameId", dt.Text(), **DEFAULT_CONTAINER_ARGS),
                    "descriptionId": MappedProperty(CONTAINER_ID, "descriptionId", dt.Text(), **DEFAULT_CONTAINER_ARGS),
                    "categoryNoId": MappedProperty(CONTAINER_ID, "categoryNoId", dt.Int64(), **DEFAULT_CONTAINER_ARGS),
                },
                {"name": "nameId", "description": "descriptionId", "metadata.categoryNo": "categoryNoId"},
                {"nameId": "MyAsset", "descriptionId": "An asset,", "categoryNoId": 1},
                ConversionIssue(asset_centric_id=ASSET_CENTRIC_ID, instance_id=INSTANCE_ID),
                id="Basic property mapping with integer conversion and no issues",
            ),
            pytest.param(
                {"name": "MyAsset", "created": "2023-01-01T12:00:00Z", "active": True},
                {
                    "nameId": MappedProperty(CONTAINER_ID, "nameId", dt.Text(), **DEFAULT_CONTAINER_ARGS),
                    "createdId": MappedProperty(CONTAINER_ID, "createdId", dt.Timestamp(), **DEFAULT_CONTAINER_ARGS),
                    "activeId": MappedProperty(CONTAINER_ID, "activeId", dt.Boolean(), **DEFAULT_CONTAINER_ARGS),
                },
                {"name": "nameId", "created": "createdId", "active": "activeId"},
                {
                    "nameId": "MyAsset",
                    "createdId": datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
                    "activeId": True,
                },
                ConversionIssue(asset_centric_id=ASSET_CENTRIC_ID, instance_id=INSTANCE_ID),
                id="Multiple data types conversion",
            ),
            pytest.param(
                {"name": "MyAsset"},
                {
                    "nameId": MappedProperty(CONTAINER_ID, "nameId", dt.Text(), **DEFAULT_CONTAINER_ARGS),
                    "descriptionId": MappedProperty(CONTAINER_ID, "descriptionId", dt.Text(), **DEFAULT_CONTAINER_ARGS),
                },
                {"name": "nameId", "description": "descriptionId"},
                {"nameId": "MyAsset"},
                ConversionIssue(
                    asset_centric_id=ASSET_CENTRIC_ID,
                    instance_id=INSTANCE_ID,
                    missing_asset_centric_properties=["description"],
                ),
                id="Missing property in flattened dump",
            ),
            pytest.param(
                {"name": "MyAsset", "description": "Test"},
                {
                    "nameId": MappedProperty(CONTAINER_ID, "nameId", dt.Text(), **DEFAULT_CONTAINER_ARGS),
                },
                {"name": "nameId", "description": "descriptionId"},
                {"nameId": "MyAsset"},
                ConversionIssue(
                    asset_centric_id=ASSET_CENTRIC_ID,
                    instance_id=INSTANCE_ID,
                    missing_instance_properties=["descriptionId"],
                ),
                id="Missing property in view properties",
            ),
            pytest.param(
                {"name": "MyAsset"},
                {
                    "nameId": MultiEdgeConnection(
                        type=DirectRelationReference("space", "Other"),
                        source=ViewId("space", "view"),
                        name=None,
                        description=None,
                        edge_source=None,
                        direction="outwards",
                    ),
                },
                {"name": "nameId"},
                {},
                ConversionIssue(
                    asset_centric_id=ASSET_CENTRIC_ID,
                    instance_id=INSTANCE_ID,
                    invalid_instance_property_types=[
                        InvalidPropertyDataType(property_id="nameId", expected_type="MappedProperty")
                    ],
                ),
                id="Invalid property type",
            ),
            pytest.param(
                {"number": "not-a-number"},
                {
                    "numberId": MappedProperty(CONTAINER_ID, "numberId", dt.Int64(), **DEFAULT_CONTAINER_ARGS),
                },
                {"number": "numberId"},
                {},
                ConversionIssue(
                    asset_centric_id=ASSET_CENTRIC_ID,
                    instance_id=INSTANCE_ID,
                    failed_conversions=[
                        FailedConversion(
                            property_id="number", value="not-a-number", error="Cannot convert not-a-number to int64."
                        )
                    ],
                ),
                id="Conversion error",
            ),
            pytest.param(
                {},
                {
                    "nameId": MappedProperty(CONTAINER_ID, "nameId", dt.Text(), **DEFAULT_CONTAINER_ARGS),
                },
                {"name": "nameId"},
                {},
                ConversionIssue(
                    asset_centric_id=ASSET_CENTRIC_ID,
                    instance_id=INSTANCE_ID,
                    missing_asset_centric_properties=["name"],
                ),
                id="Empty dictionary",
            ),
            pytest.param(
                {"name": "MyAsset", "labels": [{"externalId": "tag1"}, {"externalId": "tag2"}]},
                {
                    "nameId": MappedProperty(CONTAINER_ID, "nameId", dt.Text(), **DEFAULT_CONTAINER_ARGS),
                    "tags": MappedProperty(
                        ContainerId("cdf_cdm", "CogniteDescribable"),
                        "tags",
                        dt.Text(is_list=True),
                        **DEFAULT_CONTAINER_ARGS,
                    ),
                },
                {"name": "nameId", "labels": "tags"},
                {"nameId": "MyAsset", "tags": ["tag1", "tag2"]},
                ConversionIssue(
                    asset_centric_id=ASSET_CENTRIC_ID,
                    instance_id=INSTANCE_ID,
                ),
                id="List of simple types (labels to list of strings)",
            ),
            pytest.param(
                {
                    "name": "MyAsset",
                    "labels": [{"externalId": "tag1"}, {"externalId": "tag2"}],
                    "metadata": {"key": "value"},
                },
                {
                    "nameId": MappedProperty(CONTAINER_ID, "tags", dt.Text(), **DEFAULT_CONTAINER_ARGS),
                    "tag": MappedProperty(CONTAINER_ID, "tag", dt.Text(is_list=False), **DEFAULT_CONTAINER_ARGS),
                    "metadata": MappedProperty(CONTAINER_ID, "metadata", dt.Json(), **DEFAULT_CONTAINER_ARGS),
                },
                {"name": "nameId", "labels[0].externalId": "tag", "metadata": "metadata"},
                {"nameId": "MyAsset", "tag": "tag1", "metadata": {"key": "value"}},
                ConversionIssue(
                    asset_centric_id=ASSET_CENTRIC_ID,
                    instance_id=INSTANCE_ID,
                    ignored_asset_centric_properties=["labels[1].externalId"],
                ),
                id="Mapping the first label and entire metadata.",
            ),
            pytest.param(
                {
                    "name": "MyAsset",
                    "type": "TypeA",
                    "metadata": {"category": "TypeB"},
                },
                {
                    "nameId": MappedProperty(CONTAINER_ID, "tags", dt.Text(), **DEFAULT_CONTAINER_ARGS),
                    "tag": MappedProperty(CONTAINER_ID, "tag", dt.Text(is_list=False), **DEFAULT_CONTAINER_ARGS),
                },
                {"name": "nameId", "type": "tag", "metadata.category": "tag"},
                {"nameId": "MyAsset", "tag": "TypeA"},
                ConversionIssue(
                    asset_centric_id=ASSET_CENTRIC_ID,
                    instance_id=INSTANCE_ID,
                    ignored_asset_centric_properties=["metadata.category"],
                ),
                id="Duplicated mapping target",
            ),
        ],
    )
    def test_create_properties(
        self,
        dumped: dict[str, Any],
        view_properties: dict[str, ViewProperty],
        property_mapping: dict[str, str],
        expected_properties: dict[str, PropertyValueWrite],
        expected_issue: ConversionIssue,
    ) -> None:
        issue = ConversionIssue(asset_centric_id=self.ASSET_CENTRIC_ID, instance_id=self.INSTANCE_ID)
        properties = create_properties(
            dumped, view_properties, property_mapping, "asset", issue, self.DIRECT_RELATION_CACHE
        )

        assert properties == expected_properties

        assert issue.dump() == expected_issue.dump()

    @pytest.mark.parametrize(
        "dumped,view_properties,property_mapping,expected_properties,expected_issue",
        [
            pytest.param(
                {"startTime": 123, "endTime": 321, "description": "An event", "source": "sourceA", "assetIds": [1]},
                {
                    "startTimeId": MappedProperty(
                        CONTAINER_ID, "startTimeId", dt.Timestamp(), **DEFAULT_CONTAINER_ARGS
                    ),
                    "endTimeId": MappedProperty(CONTAINER_ID, "endTimeId", dt.Timestamp(), **DEFAULT_CONTAINER_ARGS),
                    "descriptionId": MappedProperty(CONTAINER_ID, "descriptionId", dt.Text(), **DEFAULT_CONTAINER_ARGS),
                    "source": MappedProperty(
                        ContainerId("cdf_cdm", "CogniteSourceable"),
                        "source",
                        dt.DirectRelation(),
                        **DEFAULT_CONTAINER_ARGS,
                    ),
                    "assets": MappedProperty(
                        CONTAINER_ID, "assets", dt.DirectRelation(is_list=True), **DEFAULT_CONTAINER_ARGS
                    ),
                },
                {
                    "startTime": "startTimeId",
                    "endTime": "endTimeId",
                    "description": "descriptionId",
                    "source": "source",
                    "assetIds": "assets",
                },
                {
                    "startTimeId": datetime(1970, 1, 1, 0, 0, 0, 123000, tzinfo=timezone.utc),
                    "endTimeId": datetime(1970, 1, 1, 0, 0, 0, 321000, tzinfo=timezone.utc),
                    "descriptionId": "An event",
                    "source": DirectRelationReference("instance_space", "TheSourceA"),
                    "assets": [DirectRelationReference("instance_space", "MyFirstAsset")],
                },
                ConversionIssue(asset_centric_id=EVENT_CENTRIC_ID, instance_id=INSTANCE_ID),
                id="Basic event property mapping with direct relations",
            ),
        ],
    )
    def test_create_properties_events(
        self,
        dumped: dict[str, Any],
        view_properties: dict[str, ViewProperty],
        property_mapping: dict[str, str],
        expected_properties: dict[str, PropertyValueWrite],
        expected_issue: ConversionIssue,
    ) -> None:
        issue = ConversionIssue(asset_centric_id=self.EVENT_CENTRIC_ID, instance_id=self.INSTANCE_ID)
        properties = create_properties(
            dumped, view_properties, property_mapping, "event", issue, self.DIRECT_RELATION_CACHE
        )

        assert properties == expected_properties

        assert issue.dump() == expected_issue.dump()


class TestAssetCentricConversion:
    INSTANCE_ID = NodeId(space="test_space", external_id="test_instance")
    CONTAINER_ID = ContainerId("test_space", "test_container")
    VIEW_ID = ViewId("test_space", "test_view", "v1")
    INSTANCE_SOURCE_VIEW_ID = ViewId("cognite_migration", "InstanceSource", "v1")
    ASSET_INSTANCE_ID_BY_ID: ClassVar[Mapping[int, DirectRelationReference]] = {
        123: DirectRelationReference("test_space", "asset_123_instance")
    }
    SOURCE_SYSTEM_INSTANCE_ID_BY_EXTERNAL_ID: ClassVar[Mapping[str, DirectRelationReference]] = {
        "source_system_1": DirectRelationReference("test_space", "source_system_1_instance")
    }

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
                    source="source_system_1",
                    asset_ids=[123],
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
                        "source": "source",
                        "assetIds": "assets",
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
                    "source": MappedProperty(
                        ContainerId("cdf_cdm", "CogniteSourceable"),
                        "source",
                        dt.DirectRelation(),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                    ),
                    "assets": MappedProperty(
                        ContainerId("cdf_cdm", "CogniteFile"),
                        "assets",
                        dt.DirectRelation(is_list=True, max_list_size=1200),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                    ),
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
                    "source": DirectRelationReference("test_space", "source_system_1_instance"),
                    "assets": [DirectRelationReference("test_space", "asset_123_instance")],
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
                TimeSeries(
                    id=654,
                    name="Test TimeSeries",
                    description="A test timeseries",
                    metadata=None,
                ),
                ResourceViewMapping(
                    external_id="timeseries_mapping",
                    version=1,
                    last_updated_time=1000000,
                    created_time=1000000,
                    resource_type="timeseries",
                    view_id=ViewId("test_space", "test_view", "v1"),
                    property_mapping={"name": "timeSeriesName", "metadata.category": "timeSeriesCategory"},
                ),
                {
                    "timeSeriesName": MappedProperty(
                        ContainerId("test_space", "test_container"),
                        "timeSeriesName",
                        dt.Text(),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                    ),
                    "timeSeriesCategory": MappedProperty(
                        ContainerId("test_space", "test_container"),
                        "timeSeriesCategory",
                        dt.Text(),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                    ),
                },
                {"timeSeriesName": "Test TimeSeries"},
                ConversionIssue(
                    asset_centric_id=AssetCentricId("timeseries", id_=654),
                    instance_id=INSTANCE_ID,
                    ignored_asset_centric_properties=["description"],
                    missing_asset_centric_properties=["metadata.category"],
                    missing_instance_properties=[],
                ),
                id="TimeSeries with partial mapping",
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
                Event(
                    id=999,
                    external_id="event_999",
                    type="MyType",
                    metadata={"category": "MyCategory"},
                    source="not_existing",
                ),
                ResourceViewMapping(
                    external_id="event_mapping",
                    version=1,
                    last_updated_time=1000000,
                    created_time=1000000,
                    resource_type="event",
                    view_id=ViewId("test_space", "test_view", "v1"),
                    property_mapping={"type": "category", "metadata.category": "category", "source": "source"},
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
                    "source": MappedProperty(
                        ContainerId("cdf_cdm", "CogniteSourceable"),
                        "source",
                        dt.DirectRelation(),
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
                    failed_conversions=[
                        FailedConversion(
                            property_id="source",
                            value="not_existing",
                            error="Cannot convert 'not_existing' to "
                            "DirectRelationReference. Invalid data type "
                            "or missing in lookup.",
                        )
                    ],
                ),
                id="Event with overlapping property and metadata mapping (property takes precedence) and missing source",
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
        actual, issue = asset_centric_to_dm(
            resource,
            self.INSTANCE_ID,
            view_source,
            view_properties,
            self.ASSET_INSTANCE_ID_BY_ID,
            self.SOURCE_SYSTEM_INSTANCE_ID_BY_EXTERNAL_ID,
            {},
        )

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

    ANNOTATION_ID = EdgeId(space="test_space", external_id="annotation_37")
    DEFAULT_PROPERTIES: ClassVar[dict[str, Any]] = dict(nullable=True, immutable=False, auto_increment=False)

    DIAGRAM_ANNOTATION_PROPERTIES: ClassVar[dict[str, ViewProperty]] = {
        "status": MappedProperty(
            container=CONTAINER_ID,
            container_property_identifier="status",
            type=dt.Enum({"Suggested": EnumValue(), "Approved": EnumValue(), "Rejected": EnumValue()}),
            **DEFAULT_PROPERTIES,
        ),
    }
    ANNOTATION_MAPPING = ResourceViewMapping(
        external_id="file_annotation_mapping",
        view_id=ViewId("cdf_cdm", "CogniteDiagramAnnotation", "v1"),
        property_mapping={
            "annotatedResourceId": "edge.startNode",
            "annotationType": "edge.type.externalId",
            "status": "status",
            "data.assetRef.id": "edge.endNode",
            "data.assetRef.externalId": "edge.endNode",
            "data.text": "edge.invalidProp",
        },
        version=1,
        last_updated_time=1000000,
        created_time=1000000,
        resource_type="fileAnnotation",
    )
    FILE_INSTANCE_BY_ID: ClassVar[Mapping[int, DirectRelationReference]] = {
        42: DirectRelationReference("test_space", "file_456_instance")
    }

    @pytest.mark.parametrize(
        "resource,mapping,expected_edge,expected_issue",
        [
            pytest.param(
                Annotation(
                    id=37,
                    annotated_resource_type="file",
                    annotation_type="diagrams.FileLink",
                    annotated_resource_id=42,
                    creating_user="user_1",
                    creating_app="app_1",
                    creating_app_version="1.0.0",
                    status="Approved",
                    data=dict(assetRef=dict(id=123)),
                ),
                ANNOTATION_MAPPING,
                EdgeApply(
                    space=ANNOTATION_ID.space,
                    external_id=ANNOTATION_ID.external_id,
                    start_node=DirectRelationReference("test_space", "file_456_instance"),
                    end_node=DirectRelationReference("test_space", "asset_123_instance"),
                    type=DirectRelationReference("test_space", "diagrams.FileLink"),
                    sources=[
                        NodeOrEdgeData(
                            source=ViewId("cdf_cdm", "CogniteDiagramAnnotation", "v1"),
                            properties={"status": "Approved"},
                        )
                    ],
                ),
                ConversionIssue(
                    asset_centric_id=AssetCentricId("fileAnnotation", id_=37),
                    instance_id=EdgeId(space="test_space", external_id="annotation_37"),
                    ignored_asset_centric_properties=[
                        "annotatedResourceType",
                        "creatingApp",
                        "creatingAppVersion",
                        "creatingUser",
                    ],
                    missing_asset_centric_properties=["data.assetRef.externalId", "data.text"],
                ),
                id="Basic annotation conversion",
            )
        ],
    )
    def test_asset_centric_to_annotation(
        self,
        resource: Annotation,
        mapping: ResourceViewMapping,
        expected_edge: EdgeApply,
        expected_issue: ConversionIssue,
    ) -> None:
        """Testing that asset_centric_to_dm raises can convert asset annotations. Note that unlike the other resource types,
        we do not track the linage of annotations."""

        edge, issue = asset_centric_to_dm(
            resource,
            self.ANNOTATION_ID,
            mapping,
            self.DIAGRAM_ANNOTATION_PROPERTIES,
            self.ASSET_INSTANCE_ID_BY_ID,
            {},
            self.FILE_INSTANCE_BY_ID,
        )

        assert edge.dump() == expected_edge.dump()
        assert issue.dump() == expected_issue.dump()

    def test_asset_centric_to_annotation_failed(self) -> None:
        """Testing that asset_centric_to_dm raises conversion issues for annotations with missing required properties."""

        resource = Annotation(
            id=38,
            annotated_resource_type="file",
            annotation_type="diagrams.FileLink",
            annotated_resource_id=999,  # Missing file instance
            creating_user="user_1",
            creating_app="app_1",
            creating_app_version="1.0.0",
            status="Approved",
            data=dict(assetRef=dict(id=123), text="Some annotation text"),
        )

        edge, issue = asset_centric_to_dm(
            resource,
            EdgeId(space="test_space", external_id="annotation_38"),
            self.ANNOTATION_MAPPING,
            self.DIAGRAM_ANNOTATION_PROPERTIES,
            self.ASSET_INSTANCE_ID_BY_ID,
            {},
            self.FILE_INSTANCE_BY_ID,
        )

        expected_issue = ConversionIssue(
            asset_centric_id=AssetCentricId("fileAnnotation", id_=38),
            instance_id=EdgeId(space="test_space", external_id="annotation_38"),
            ignored_asset_centric_properties=[
                "annotatedResourceType",
                "creatingApp",
                "creatingAppVersion",
                "creatingUser",
            ],
            missing_asset_centric_properties=["data.assetRef.externalId"],
            missing_instance_properties=[],
            invalid_instance_property_types=[
                InvalidPropertyDataType(property_id="edge.invalidProp", expected_type="EdgeProperty")
            ],
            failed_conversions=[
                FailedConversion(
                    property_id="annotatedResourceId",
                    value=999,
                    error="Cannot convert 999 to DirectRelationReference. Invalid data type or missing in lookup.",
                )
            ],
        )

        assert issue.dump() == expected_issue.dump()
        assert edge is None
