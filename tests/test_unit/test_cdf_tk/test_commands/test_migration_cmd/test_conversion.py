import json
from collections.abc import Mapping
from typing import Any, ClassVar

import pytest
from cognite.client.data_classes import Annotation, Sequence
from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client.resource_classes.asset import AssetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    BooleanProperty,
    ConstraintOrIndexState,
    ContainerReference,
    DirectNodeRelation,
    EdgeReference,
    EdgeRequest,
    EnumProperty,
    EnumValue,
    InstanceSource,
    Int64Property,
    JSONProperty,
    MultiEdgeProperty,
    NodeReference,
    TextProperty,
    TimestampProperty,
    ViewCorePropertyResponse,
    ViewReference,
    ViewResponseProperty,
)
from cognite_toolkit._cdf_tk.client.resource_classes.event import EventResponse
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataResponse
from cognite_toolkit._cdf_tk.client.resource_classes.legacy.migration import (
    AssetCentricId,
    CreatedSourceSystem,
    ResourceViewMapping,
)
from cognite_toolkit._cdf_tk.client.resource_classes.timeseries import TimeSeriesResponse
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
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


@pytest.fixture(scope="module")
def direct_relation_cache() -> DirectRelationCache:
    with monkeypatch_toolkit_client() as client:
        client.migration.lookup.assets.return_value = {
            123: NodeReference(space="test_space", external_id="asset_123_instance"),
            1: NodeReference(space="instance_space", external_id="MyFirstAsset"),
        }
        client.migration.lookup.files.return_value = {
            42: NodeReference(space="test_space", external_id="file_456_instance")
        }
        client.migration.created_source_system.retrieve.return_value = [
            CreatedSourceSystem(
                space="test_space",
                external_id="source_system_1_instance",
                version=1,
                last_updated_time=1,
                created_time=1,
                source="source_system_1",
            ),
            CreatedSourceSystem(
                space="instance_space",
                external_id="TheSourceA",
                version=1,
                last_updated_time=1,
                created_time=1,
                source="sourceA",
            ),
        ]
        cache = DirectRelationCache(client)
        cache.update(
            [
                EventResponse(asset_ids=[123, 1], source="source_system_1", createdTime=1, lastUpdatedTime=1, id=0),
                AssetResponse(source="SourceA", createdTime=1, lastUpdatedTime=1, rootId=0, id=0, name=""),
                Annotation("diagrams.FileLink", {}, "Accepted", "app", "app-version", "me", "file", 42, 1),
            ]
        )
    return cache


class TestCreateProperties:
    INSTANCE_ID = NodeReference(space="test_space", external_id="test_instance")
    CONTAINER_ID = ContainerReference(space="test_space", external_id="test_container")
    DEFAULT_CONTAINER_ARGS: ClassVar = dict(
        nullable=True, immutable=False, auto_increment=False, constraint_state=ConstraintOrIndexState()
    )
    ASSET_CENTRIC_ID = AssetCentricId(resource_type="asset", id_=123)
    EVENT_CENTRIC_ID = AssetCentricId(resource_type="event", id_=456)

    @pytest.mark.parametrize(
        "dumped,view_properties,property_mapping,expected_properties,expected_issue",
        [
            pytest.param(
                {"name": "MyAsset", "description": "An asset,", "metadata": {"categoryNo": "1"}},
                {
                    "nameId": ViewCorePropertyResponse(
                        container=CONTAINER_ID,
                        container_property_identifier="nameId",
                        type=TextProperty(),
                        **DEFAULT_CONTAINER_ARGS,
                    ),
                    "descriptionId": ViewCorePropertyResponse(
                        container=CONTAINER_ID,
                        container_property_identifier="descriptionId",
                        type=TextProperty(),
                        **DEFAULT_CONTAINER_ARGS,
                    ),
                    "categoryNoId": ViewCorePropertyResponse(
                        container=CONTAINER_ID,
                        container_property_identifier="categoryNoId",
                        type=Int64Property(),
                        **DEFAULT_CONTAINER_ARGS,
                    ),
                },
                {"name": "nameId", "description": "descriptionId", "metadata.categoryNo": "categoryNoId"},
                {"nameId": "MyAsset", "descriptionId": "An asset,", "categoryNoId": 1},
                ConversionIssue(id=str(ASSET_CENTRIC_ID), instance_id=INSTANCE_ID, asset_centric_id=ASSET_CENTRIC_ID),
                id="Basic property mapping with integer conversion and no issues",
            ),
            pytest.param(
                {"name": "MyAsset", "created": "2023-01-01T12:00:00Z", "active": True},
                {
                    "nameId": ViewCorePropertyResponse(
                        container=CONTAINER_ID,
                        container_property_identifier="nameId",
                        type=TextProperty(),
                        **DEFAULT_CONTAINER_ARGS,
                    ),
                    "createdId": ViewCorePropertyResponse(
                        container=CONTAINER_ID,
                        container_property_identifier="createdId",
                        type=TimestampProperty(),
                        **DEFAULT_CONTAINER_ARGS,
                    ),
                    "activeId": ViewCorePropertyResponse(
                        container=CONTAINER_ID,
                        container_property_identifier="activeId",
                        type=BooleanProperty(),
                        **DEFAULT_CONTAINER_ARGS,
                    ),
                },
                {"name": "nameId", "created": "createdId", "active": "activeId"},
                {
                    "nameId": "MyAsset",
                    "createdId": "2023-01-01T12:00:00+00:00",
                    "activeId": True,
                },
                ConversionIssue(id=str(ASSET_CENTRIC_ID), asset_centric_id=ASSET_CENTRIC_ID, instance_id=INSTANCE_ID),
                id="Multiple data types conversion",
            ),
            pytest.param(
                {"name": "MyAsset"},
                {
                    "nameId": ViewCorePropertyResponse(
                        container=CONTAINER_ID,
                        container_property_identifier="nameId",
                        type=TextProperty(),
                        **DEFAULT_CONTAINER_ARGS,
                    ),
                    "descriptionId": ViewCorePropertyResponse(
                        container=CONTAINER_ID,
                        container_property_identifier="descriptionId",
                        type=TextProperty(),
                        **DEFAULT_CONTAINER_ARGS,
                    ),
                },
                {"name": "nameId", "description": "descriptionId"},
                {"nameId": "MyAsset"},
                ConversionIssue(
                    id=str(ASSET_CENTRIC_ID),
                    asset_centric_id=ASSET_CENTRIC_ID,
                    instance_id=INSTANCE_ID,
                    missing_asset_centric_properties=["description"],
                ),
                id="Missing property in flattened dump",
            ),
            pytest.param(
                {"name": "MyAsset", "description": "Test"},
                {
                    "nameId": ViewCorePropertyResponse(
                        container=CONTAINER_ID,
                        container_property_identifier="nameId",
                        type=TextProperty(),
                        **DEFAULT_CONTAINER_ARGS,
                    ),
                },
                {"name": "nameId", "description": "descriptionId"},
                {"nameId": "MyAsset"},
                ConversionIssue(
                    id=str(ASSET_CENTRIC_ID),
                    asset_centric_id=ASSET_CENTRIC_ID,
                    instance_id=INSTANCE_ID,
                    missing_instance_properties=["descriptionId"],
                ),
                id="Missing property in view properties",
            ),
            pytest.param(
                {"name": "MyAsset"},
                {
                    "nameId": MultiEdgeProperty(
                        type=NodeReference(space="space", external_id="Other"),
                        source=ViewReference(space="space", external_id="view", version="v1"),
                        name=None,
                        description=None,
                        edge_source=None,
                        direction="outwards",
                    ),
                },
                {"name": "nameId"},
                {},
                ConversionIssue(
                    id=str(ASSET_CENTRIC_ID),
                    asset_centric_id=ASSET_CENTRIC_ID,
                    instance_id=INSTANCE_ID,
                    invalid_instance_property_types=[
                        InvalidPropertyDataType(property_id="nameId", expected_type="ViewCorePropertyResponse")
                    ],
                ),
                id="Invalid property type",
            ),
            pytest.param(
                {"number": "not-a-number"},
                {
                    "numberId": ViewCorePropertyResponse(
                        container=CONTAINER_ID,
                        container_property_identifier="numberId",
                        type=Int64Property(),
                        **DEFAULT_CONTAINER_ARGS,
                    ),
                },
                {"number": "numberId"},
                {},
                ConversionIssue(
                    id=str(ASSET_CENTRIC_ID),
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
                    "nameId": ViewCorePropertyResponse(
                        container=CONTAINER_ID,
                        container_property_identifier="nameId",
                        type=TextProperty(),
                        **DEFAULT_CONTAINER_ARGS,
                    ),
                },
                {"name": "nameId"},
                {},
                ConversionIssue(
                    id=str(ASSET_CENTRIC_ID),
                    asset_centric_id=ASSET_CENTRIC_ID,
                    instance_id=INSTANCE_ID,
                    missing_asset_centric_properties=["name"],
                ),
                id="Empty dictionary",
            ),
            pytest.param(
                {"name": "MyAsset", "labels": [{"externalId": "tag1"}, {"externalId": "tag2"}]},
                {
                    "nameId": ViewCorePropertyResponse(
                        container=CONTAINER_ID,
                        container_property_identifier="nameId",
                        type=TextProperty(),
                        **DEFAULT_CONTAINER_ARGS,
                    ),
                    "tags": ViewCorePropertyResponse(
                        container=ContainerReference(space="cdf_cdm", external_id="CogniteDescribable"),
                        container_property_identifier="tags",
                        type=TextProperty(list=True),
                        **DEFAULT_CONTAINER_ARGS,
                    ),
                },
                {"name": "nameId", "labels": "tags"},
                {"nameId": "MyAsset", "tags": ["tag1", "tag2"]},
                ConversionIssue(
                    id=str(ASSET_CENTRIC_ID),
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
                    "nameId": ViewCorePropertyResponse(
                        container=CONTAINER_ID,
                        container_property_identifier="tags",
                        type=TextProperty(),
                        **DEFAULT_CONTAINER_ARGS,
                    ),
                    "tag": ViewCorePropertyResponse(
                        container=CONTAINER_ID,
                        container_property_identifier="tag",
                        type=TextProperty(list=False),
                        **DEFAULT_CONTAINER_ARGS,
                    ),
                    "metadata": ViewCorePropertyResponse(
                        container=CONTAINER_ID,
                        container_property_identifier="metadata",
                        type=JSONProperty(),
                        **DEFAULT_CONTAINER_ARGS,
                    ),
                },
                {"name": "nameId", "labels[0].externalId": "tag", "metadata": "metadata"},
                {"nameId": "MyAsset", "tag": "tag1", "metadata": {"key": "value"}},
                ConversionIssue(
                    id=str(ASSET_CENTRIC_ID),
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
                    "nameId": ViewCorePropertyResponse(
                        container=CONTAINER_ID,
                        container_property_identifier="tags",
                        type=TextProperty(),
                        **DEFAULT_CONTAINER_ARGS,
                    ),
                    "tag": ViewCorePropertyResponse(
                        container=CONTAINER_ID,
                        container_property_identifier="tag",
                        type=TextProperty(list=False),
                        **DEFAULT_CONTAINER_ARGS,
                    ),
                },
                {"name": "nameId", "type": "tag", "metadata.category": "tag"},
                {"nameId": "MyAsset", "tag": "TypeA"},
                ConversionIssue(
                    id=str(ASSET_CENTRIC_ID),
                    asset_centric_id=ASSET_CENTRIC_ID,
                    instance_id=INSTANCE_ID,
                    ignored_asset_centric_properties=["metadata.category"],
                ),
                id="Duplicated mapping target",
            ),
            pytest.param(
                {
                    "name": "MyAsset",
                    "metadata": {"雪ヘ罪約べげド. [10] SNL": "値テスト"},
                },
                {
                    "name": ViewCorePropertyResponse(
                        container=CONTAINER_ID,
                        container_property_identifier="name",
                        type=TextProperty(),
                        **DEFAULT_CONTAINER_ARGS,
                    ),
                    "property10": ViewCorePropertyResponse(
                        container=CONTAINER_ID,
                        container_property_identifier="property10",
                        type=TextProperty(),
                        **DEFAULT_CONTAINER_ARGS,
                    ),
                },
                {"name": "name", "metadata.雪ヘ罪約べげド. [10] SNL": "property10"},
                {"name": "MyAsset", "property10": "値テスト"},
                ConversionIssue(
                    id=str(ASSET_CENTRIC_ID),
                    asset_centric_id=ASSET_CENTRIC_ID,
                    instance_id=INSTANCE_ID,
                ),
                id="Japanese characters in property names and values",
            ),
        ],
    )
    def test_create_properties(
        self,
        dumped: dict[str, Any],
        view_properties: dict[str, ViewResponseProperty],
        property_mapping: dict[str, str],
        expected_properties: dict[str, JsonValue],
        expected_issue: ConversionIssue,
        direct_relation_cache: DirectRelationCache,
    ) -> None:
        issue = ConversionIssue(
            asset_centric_id=self.ASSET_CENTRIC_ID, instance_id=self.INSTANCE_ID, id=str(self.ASSET_CENTRIC_ID)
        )

        properties = create_properties(dumped, view_properties, property_mapping, "asset", issue, direct_relation_cache)

        assert properties == expected_properties

        assert issue.dump() == expected_issue.dump()

    @pytest.mark.parametrize(
        "dumped,view_properties,property_mapping,expected_properties,expected_issue",
        [
            pytest.param(
                {"startTime": 123, "endTime": 321, "description": "An event", "source": "sourceA", "assetIds": [1]},
                {
                    "startTimeId": ViewCorePropertyResponse(
                        container=CONTAINER_ID,
                        container_property_identifier="startTimeId",
                        type=TimestampProperty(),
                        **DEFAULT_CONTAINER_ARGS,
                    ),
                    "endTimeId": ViewCorePropertyResponse(
                        container=CONTAINER_ID,
                        container_property_identifier="endTimeId",
                        type=TimestampProperty(),
                        **DEFAULT_CONTAINER_ARGS,
                    ),
                    "descriptionId": ViewCorePropertyResponse(
                        container=CONTAINER_ID,
                        container_property_identifier="descriptionId",
                        type=TextProperty(),
                        **DEFAULT_CONTAINER_ARGS,
                    ),
                    "source": ViewCorePropertyResponse(
                        container=ContainerReference(space="cdf_cdm", external_id="CogniteSourceable"),
                        container_property_identifier="source",
                        type=DirectNodeRelation(),
                        **DEFAULT_CONTAINER_ARGS,
                    ),
                    "assets": ViewCorePropertyResponse(
                        container=CONTAINER_ID,
                        container_property_identifier="assets",
                        type=DirectNodeRelation(list=True),
                        **DEFAULT_CONTAINER_ARGS,
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
                    "startTimeId": "1970-01-01T00:00:00.123000+00:00",
                    "endTimeId": "1970-01-01T00:00:00.321000+00:00",
                    "descriptionId": "An event",
                    "source": {"space": "instance_space", "externalId": "TheSourceA"},
                    "assets": [{"space": "instance_space", "externalId": "MyFirstAsset"}],
                },
                ConversionIssue(id=str(EVENT_CENTRIC_ID), asset_centric_id=EVENT_CENTRIC_ID, instance_id=INSTANCE_ID),
                id="Basic event property mapping with direct relations",
            ),
        ],
    )
    def test_create_properties_events(
        self,
        dumped: dict[str, Any],
        view_properties: dict[str, ViewResponseProperty],
        property_mapping: dict[str, str],
        expected_properties: dict[str, JsonValue],
        expected_issue: ConversionIssue,
        direct_relation_cache: DirectRelationCache,
    ) -> None:
        issue = ConversionIssue(
            asset_centric_id=self.EVENT_CENTRIC_ID, instance_id=self.INSTANCE_ID, id=str(self.EVENT_CENTRIC_ID)
        )
        properties = create_properties(dumped, view_properties, property_mapping, "event", issue, direct_relation_cache)

        assert properties == expected_properties

        assert issue.dump() == expected_issue.dump()


class TestAssetCentricConversion:
    INSTANCE_ID = NodeReference(space="test_space", external_id="test_instance")
    INSTANCE_REF = NodeReference(space="test_space", external_id="test_instance")
    CONTAINER_ID = ContainerReference(space="test_space", external_id="test_container")
    VIEW_ID = ViewReference(space="test_space", external_id="test_view", version="v1")
    INSTANCE_SOURCE_VIEW_ID = ViewReference(space="cognite_migration", external_id="InstanceSource", version="v1")
    ASSET_INSTANCE_ID_BY_ID: ClassVar[Mapping[int, NodeReference]] = {
        123: NodeReference(space="test_space", external_id="asset_123_instance")
    }
    SOURCE_SYSTEM_INSTANCE_ID_BY_EXTERNAL_ID: ClassVar[Mapping[str, NodeReference]] = {
        "source_system_1": NodeReference(space="test_space", external_id="source_system_1_instance")
    }

    @pytest.mark.parametrize(
        "resource,view_source,view_properties,expected_properties,expected_issue",
        [
            pytest.param(
                # Simple Asset with basic mapping
                AssetResponse(
                    id=123,
                    externalId="asset_123",
                    name="Test Asset",
                    description="A test asset",
                    createdTime=1,
                    lastUpdatedTime=1,
                    rootId=0,
                ),
                ResourceViewMapping(
                    external_id="asset_mapping",
                    version=1,
                    last_updated_time=1000000,
                    created_time=1000000,
                    resource_type="asset",
                    view_id=ViewReference(space="test_space", external_id="test_view", version="v1"),
                    property_mapping={"name": "assetName", "description": "assetDescription"},
                ),
                {
                    "assetName": ViewCorePropertyResponse(
                        container=ContainerReference(space="test_space", external_id="test_container"),
                        container_property_identifier="assetName",
                        type=TextProperty(),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                        constraint_state=ConstraintOrIndexState(),
                    ),
                    "assetDescription": ViewCorePropertyResponse(
                        container=ContainerReference(space="test_space", external_id="test_container"),
                        container_property_identifier="assetDescription",
                        type=TextProperty(),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                        constraint_state=ConstraintOrIndexState(),
                    ),
                },
                {"assetName": "Test Asset", "assetDescription": "A test asset"},
                ConversionIssue(
                    id=str(AssetCentricId("asset", 123)),
                    asset_centric_id=AssetCentricId("asset", 123),
                    instance_id=INSTANCE_REF,
                    ignored_asset_centric_properties=["createdTime", "lastUpdatedTime", "rootId"],
                ),
                id="simple_asset_mapping",
            ),
            pytest.param(
                TimeSeriesResponse(
                    id=456,
                    external_id="ts_456",
                    name="Test TimeSeries",
                    description="A test timeseries",
                    unit="celsius",
                    metadata={"sensor_type": "temperature", "location": "room_1"},
                    is_string=False,
                    is_step=False,
                    created_time=0,
                    last_updated_time=0,
                    type="numeric",
                ),
                ResourceViewMapping(
                    external_id="timeseries_mapping",
                    version=1,
                    last_updated_time=1000000,
                    created_time=1000000,
                    resource_type="timeseries",
                    view_id=ViewReference(space="test_space", external_id="test_view", version="v1"),
                    property_mapping={
                        "name": "timeseriesName",
                        "unit": "measurementUnit",
                        "metadata.sensor_type": "sensorType",
                        "metadata.location": "deviceLocation",
                    },
                ),
                {
                    "timeseriesName": ViewCorePropertyResponse(
                        container=ContainerReference(space="test_space", external_id="test_container"),
                        container_property_identifier="timeseriesName",
                        type=TextProperty(),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                        constraint_state=ConstraintOrIndexState(),
                    ),
                    "measurementUnit": ViewCorePropertyResponse(
                        container=ContainerReference(space="test_space", external_id="test_container"),
                        container_property_identifier="measurementUnit",
                        type=TextProperty(),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                        constraint_state=ConstraintOrIndexState(),
                    ),
                    "sensorType": ViewCorePropertyResponse(
                        container=ContainerReference(space="test_space", external_id="test_container"),
                        container_property_identifier="sensorType",
                        type=TextProperty(),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                        constraint_state=ConstraintOrIndexState(),
                    ),
                    "deviceLocation": ViewCorePropertyResponse(
                        container=ContainerReference(space="test_space", external_id="test_container"),
                        container_property_identifier="deviceLocation",
                        type=TextProperty(),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                        constraint_state=ConstraintOrIndexState(),
                    ),
                },
                {
                    "timeseriesName": "Test TimeSeries",
                    "measurementUnit": "celsius",
                    "sensorType": "temperature",
                    "deviceLocation": "room_1",
                },
                ConversionIssue(
                    id=str(AssetCentricId("timeseries", 456)),
                    asset_centric_id=AssetCentricId("timeseries", id_=456),
                    instance_id=INSTANCE_REF,
                    ignored_asset_centric_properties=[
                        "createdTime",
                        "description",
                        "isStep",
                        "isString",
                        "lastUpdatedTime",
                        "type",
                    ],
                ),
                id="timeseries_with_metadata",
            ),
            pytest.param(
                EventResponse(
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
                    created_time=1,
                    last_updated_time=1,
                ),
                ResourceViewMapping(
                    external_id="incomplete_mapping",
                    version=1,
                    last_updated_time=1000000,
                    created_time=1000000,
                    resource_type="event",
                    view_id=ViewReference(space="test_space", external_id="test_view", version="v1"),
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
                    "source": ViewCorePropertyResponse(
                        container=ContainerReference(space="cdf_cdm", external_id="CogniteSourceable"),
                        container_property_identifier="source",
                        type=DirectNodeRelation(),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                        constraint_state=ConstraintOrIndexState(),
                    ),
                    "assets": ViewCorePropertyResponse(
                        container=ContainerReference(space="cdf_cdm", external_id="CogniteFile"),
                        container_property_identifier="assets",
                        type=DirectNodeRelation(list=True, max_list_size=1200),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                        constraint_state=ConstraintOrIndexState(),
                    ),
                    "eventStart": ViewCorePropertyResponse(
                        container=ContainerReference(space="test_space", external_id="test_container"),
                        container_property_identifier="eventStart",
                        type=TimestampProperty(),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                        constraint_state=ConstraintOrIndexState(),
                    ),
                    "eventEnd": ViewCorePropertyResponse(
                        container=ContainerReference(space="test_space", external_id="test_container"),
                        container_property_identifier="eventEnd",
                        type=TimestampProperty(),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                        constraint_state=ConstraintOrIndexState(),
                    ),
                    "eventSeverity": ViewCorePropertyResponse(
                        container=ContainerReference(space="test_space", external_id="test_container"),
                        container_property_identifier="eventSeverity",
                        type=EnumProperty(values={"high": EnumValue(), "low": EnumValue(), "medium": EnumValue()}),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                        constraint_state=ConstraintOrIndexState(),
                    ),
                    "eventValue": ViewCorePropertyResponse(
                        container=ContainerReference(space="test_space", external_id="test_container"),
                        container_property_identifier="eventValue",
                        type=Int64Property(),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                        constraint_state=ConstraintOrIndexState(),
                    ),
                    "some_other_event": MultiEdgeProperty(
                        type=NodeReference(space="schema_space", external_id="Operation"),
                        source=ViewReference(space="test_space", external_id="test_view", version="v1"),
                        name=None,
                        description=None,
                        edge_source=None,
                        direction="outwards",
                    ),
                },
                {
                    "eventStart": "2025-08-28T05:38:09.386000+00:00",
                    "eventEnd": "2025-08-28T05:38:19.880000+00:00",
                    "eventSeverity": "high",
                    "source": {"space": "test_space", "externalId": "source_system_1_instance"},
                    "assets": [{"space": "test_space", "externalId": "asset_123_instance"}],
                },
                ConversionIssue(
                    id=str(AssetCentricId("event", 789)),
                    asset_centric_id=AssetCentricId("event", id_=789),
                    instance_id=INSTANCE_REF,
                    ignored_asset_centric_properties=["createdTime", "description", "lastUpdatedTime"],
                    missing_asset_centric_properties=["metadata.missingMetaProp", "missing_prop"],
                    missing_instance_properties=["anotherMissingDMProp", "missingDMProp", "targetProp"],
                    invalid_instance_property_types=[
                        InvalidPropertyDataType(
                            property_id="some_other_event", expected_type="ViewCorePropertyResponse"
                        )
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
                FileMetadataResponse(
                    id=321,
                    external_id="file_321",
                    name="Test File",
                    mime_type="application/octet-stream",
                    metadata={"file_type": "pdf", "confidential": "true"},
                    created_time=0,
                    last_updated_time=0,
                    uploaded=True,
                ),
                ResourceViewMapping(
                    external_id="file_mapping",
                    version=1,
                    last_updated_time=1000000,
                    created_time=1000000,
                    resource_type="file",
                    view_id=ViewReference(space="test_space", external_id="test_view", version="v1"),
                    property_mapping={},
                ),
                {
                    "fileName": ViewCorePropertyResponse(
                        container=ContainerReference(space="test_space", external_id="test_container"),
                        container_property_identifier="fileName",
                        type=TextProperty(),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                        constraint_state=ConstraintOrIndexState(),
                    ),
                    "fileDescription": ViewCorePropertyResponse(
                        container=ContainerReference(space="test_space", external_id="test_container"),
                        container_property_identifier="fileDescription",
                        type=TextProperty(),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                        constraint_state=ConstraintOrIndexState(),
                    ),
                    "fileType": ViewCorePropertyResponse(
                        container=ContainerReference(space="test_space", external_id="test_container"),
                        container_property_identifier="fileType",
                        type=TextProperty(),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                        constraint_state=ConstraintOrIndexState(),
                    ),
                    "isConfidential": ViewCorePropertyResponse(
                        container=ContainerReference(space="test_space", external_id="test_container"),
                        container_property_identifier="isConfidential",
                        type=BooleanProperty(),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                        constraint_state=ConstraintOrIndexState(),
                    ),
                },
                {},
                ConversionIssue(
                    id=str(AssetCentricId("file", 321)),
                    asset_centric_id=AssetCentricId("file", id_=321),
                    instance_id=INSTANCE_REF,
                    ignored_asset_centric_properties=[
                        "createdTime",
                        "lastUpdatedTime",
                        "metadata.confidential",
                        "metadata.file_type",
                        "mimeType",
                        "name",
                        "uploaded",
                    ],
                    missing_asset_centric_properties=[],
                    missing_instance_properties=[],
                ),
                id="FileMetadata with no mappings (all ignored)",
            ),
            pytest.param(
                TimeSeriesResponse(
                    id=654,
                    name="Test TimeSeries",
                    description="A test timeseries",
                    metadata=None,
                    is_string=False,
                    is_step=False,
                    created_time=0,
                    last_updated_time=0,
                    type="numeric",
                ),
                ResourceViewMapping(
                    external_id="timeseries_mapping",
                    version=1,
                    last_updated_time=1000000,
                    created_time=1000000,
                    resource_type="timeseries",
                    view_id=ViewReference(space="test_space", external_id="test_view", version="v1"),
                    property_mapping={"name": "timeSeriesName", "metadata.category": "timeSeriesCategory"},
                ),
                {
                    "timeSeriesName": ViewCorePropertyResponse(
                        container=ContainerReference(space="test_space", external_id="test_container"),
                        container_property_identifier="timeSeriesName",
                        type=TextProperty(),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                        constraint_state=ConstraintOrIndexState(),
                    ),
                    "timeSeriesCategory": ViewCorePropertyResponse(
                        container=ContainerReference(space="test_space", external_id="test_container"),
                        container_property_identifier="timeSeriesCategory",
                        type=TextProperty(),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                        constraint_state=ConstraintOrIndexState(),
                    ),
                },
                {"timeSeriesName": "Test TimeSeries"},
                ConversionIssue(
                    id=str(AssetCentricId("timeseries", 654)),
                    asset_centric_id=AssetCentricId("timeseries", id_=654),
                    instance_id=INSTANCE_REF,
                    ignored_asset_centric_properties=[
                        "createdTime",
                        "description",
                        "isStep",
                        "isString",
                        "lastUpdatedTime",
                        "metadata",
                        "type",
                    ],
                    missing_asset_centric_properties=["metadata.category"],
                    missing_instance_properties=[],
                ),
                id="TimeSeries with partial mapping",
            ),
            pytest.param(
                AssetResponse(
                    id=999, externalId="asset_999", name="The name", createdTime=1, lastUpdatedTime=1, rootId=0
                ),
                ResourceViewMapping(
                    external_id="empty_mapping",
                    version=1,
                    last_updated_time=1000000,
                    created_time=1000000,
                    resource_type="asset",
                    view_id=ViewReference(space="test_space", external_id="test_view", version="v1"),
                    property_mapping={"name": "assetName", "description": "assetDescription"},
                ),
                {
                    "assetName": ViewCorePropertyResponse(
                        container=ContainerReference(space="test_space", external_id="test_container"),
                        container_property_identifier="assetName",
                        type=TextProperty(),
                        nullable=False,
                        immutable=False,
                        auto_increment=False,
                        constraint_state=ConstraintOrIndexState(),
                    ),
                    "assetDescription": ViewCorePropertyResponse(
                        container=ContainerReference(space="test_space", external_id="test_container"),
                        container_property_identifier="assetDescription",
                        type=TextProperty(),
                        nullable=False,
                        immutable=False,
                        auto_increment=False,
                        constraint_state=ConstraintOrIndexState(),
                    ),
                },
                {"assetName": "The name"},
                ConversionIssue(
                    id=str(AssetCentricId("asset", 999)),
                    asset_centric_id=AssetCentricId("asset", id_=999),
                    instance_id=INSTANCE_REF,
                    ignored_asset_centric_properties=["createdTime", "lastUpdatedTime", "rootId"],
                    missing_asset_centric_properties=["description"],
                    missing_instance_properties=[],
                ),
                id="Asset with non-nullable properties all None",
            ),
            pytest.param(
                EventResponse(
                    id=999,
                    external_id="event_999",
                    type="MyType",
                    metadata={"category": "MyCategory"},
                    source="not_existing",
                    created_time=0,
                    last_updated_time=1,
                ),
                ResourceViewMapping(
                    external_id="event_mapping",
                    version=1,
                    last_updated_time=1000000,
                    created_time=1000000,
                    resource_type="event",
                    view_id=ViewReference(space="test_space", external_id="test_view", version="v1"),
                    property_mapping={"type": "category", "metadata.category": "category", "source": "source"},
                ),
                {
                    "category": ViewCorePropertyResponse(
                        container=ContainerReference(space="test_space", external_id="test_container"),
                        container_property_identifier="category",
                        type=TextProperty(),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                        constraint_state=ConstraintOrIndexState(),
                    ),
                    "source": ViewCorePropertyResponse(
                        container=ContainerReference(space="cdf_cdm", external_id="CogniteSourceable"),
                        container_property_identifier="source",
                        type=DirectNodeRelation(),
                        nullable=True,
                        immutable=False,
                        auto_increment=False,
                        constraint_state=ConstraintOrIndexState(),
                    ),
                },
                {
                    "category": "MyType",
                },
                ConversionIssue(
                    id=str(AssetCentricId("event", 999)),
                    asset_centric_id=AssetCentricId("event", id_=999),
                    instance_id=INSTANCE_REF,
                    ignored_asset_centric_properties=["createdTime", "lastUpdatedTime", "metadata.category"],
                    failed_conversions=[
                        FailedConversion(
                            property_id="source",
                            value="not_existing",
                            error="Cannot convert 'not_existing' to "
                            "NodeReference. Invalid data type "
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
        resource: AssetResponse | FileMetadataResponse | EventResponse | TimeSeriesResponse | Sequence,
        view_source: ResourceViewMapping,
        view_properties: dict[str, ViewResponseProperty],
        expected_properties: dict[str, Any],
        expected_issue: ConversionIssue,
        direct_relation_cache: DirectRelationCache,
    ) -> None:
        actual, issue = asset_centric_to_dm(
            resource,
            self.INSTANCE_ID,
            view_source,
            view_properties,
            direct_relation_cache,
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

    ANNOTATION_ID = EdgeReference(space="test_space", external_id="annotation_37")
    DEFAULT_PROPERTIES: ClassVar[dict[str, Any]] = dict(
        nullable=True, immutable=False, auto_increment=False, constraint_state=ConstraintOrIndexState()
    )

    DIAGRAM_ANNOTATION_PROPERTIES: ClassVar[dict[str, ViewResponseProperty]] = {
        "status": ViewCorePropertyResponse(
            container=CONTAINER_ID,
            container_property_identifier="status",
            type=EnumProperty(values={"Suggested": EnumValue(), "Approved": EnumValue(), "Rejected": EnumValue()}),
            **DEFAULT_PROPERTIES,
        ),
    }
    ANNOTATION_MAPPING = ResourceViewMapping(
        external_id="file_annotation_mapping",
        view_id=ViewReference(space="cdf_cdm", external_id="CogniteDiagramAnnotation", version="v1"),
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
        resource_type="annotation",
    )
    FILE_INSTANCE_BY_ID: ClassVar[Mapping[int, NodeReference]] = {
        42: NodeReference(space="test_space", external_id="file_456_instance")
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
                EdgeRequest(
                    space=ANNOTATION_ID.space,
                    external_id=ANNOTATION_ID.external_id,
                    start_node=NodeReference(space="test_space", external_id="file_456_instance"),
                    end_node=NodeReference(space="test_space", external_id="asset_123_instance"),
                    type=NodeReference(space="test_space", external_id="diagrams.FileLink"),
                    sources=[
                        InstanceSource(
                            source=ViewReference(space="cdf_cdm", external_id="CogniteDiagramAnnotation", version="v1"),
                            properties={"status": "Approved"},
                        )
                    ],
                ),
                ConversionIssue(
                    id=str(AssetCentricId("annotation", 37)),
                    asset_centric_id=AssetCentricId("annotation", id_=37),
                    instance_id=NodeReference(space="test_space", external_id="annotation_37"),
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
        expected_edge: EdgeRequest,
        expected_issue: ConversionIssue,
        direct_relation_cache: DirectRelationCache,
    ) -> None:
        """Testing that asset_centric_to_dm raises can convert asset annotations. Note that unlike the other resource types,
        we do not track the linage of annotations."""

        edge, issue = asset_centric_to_dm(
            resource,
            self.ANNOTATION_ID,
            mapping,
            self.DIAGRAM_ANNOTATION_PROPERTIES,
            direct_relation_cache,
        )

        assert issue.dump() == expected_issue.dump()
        assert edge.dump() == expected_edge.dump()

    def test_asset_centric_to_annotation_failed(self, direct_relation_cache: DirectRelationCache) -> None:
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
            EdgeReference(space="test_space", external_id="annotation_38"),
            self.ANNOTATION_MAPPING,
            self.DIAGRAM_ANNOTATION_PROPERTIES,
            direct_relation_cache,
        )

        expected_issue = ConversionIssue(
            id=str(AssetCentricId("annotation", 38)),
            asset_centric_id=AssetCentricId("annotation", id_=38),
            instance_id=NodeReference(space="test_space", external_id="annotation_38"),
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
                    error="Cannot convert 999 to NodeReference. Invalid data type or missing in lookup.",
                )
            ],
        )

        assert issue.dump() == expected_issue.dump()
        assert edge is None
