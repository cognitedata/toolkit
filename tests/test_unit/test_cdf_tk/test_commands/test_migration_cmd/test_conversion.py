from datetime import datetime, timezone
from typing import Any, ClassVar

import pytest
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.data_classes.data_modeling import data_types as dt
from cognite.client.data_classes.data_modeling.data_types import DirectRelationReference
from cognite.client.data_classes.data_modeling.ids import ContainerId, ViewId
from cognite.client.data_classes.data_modeling.instances import PropertyValueWrite
from cognite.client.data_classes.data_modeling.views import MappedProperty, MultiEdgeConnection, ViewProperty

from cognite_toolkit._cdf_tk.client.data_classes.migration import AssetCentricId
from cognite_toolkit._cdf_tk.commands._migrate.conversion import create_properties
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
        properties = create_properties(dumped, view_properties, property_mapping, "asset", issue)

        assert properties == expected_properties

        assert issue.dump() == expected_issue.dump()
