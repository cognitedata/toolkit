from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, ClassVar
from unittest.mock import MagicMock

import pytest
import yaml
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling import InstanceApply
from pytest_regressions.data_regression import DataRegressionFixture

from cognite_toolkit._cdf_tk.client.identifiers import ContainerId, EdgeTypeId, NodeId, ViewDirectId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import ContainerPropertyDefinition
from cognite_toolkit._cdf_tk.client.resource_classes.asset import AssetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.canvas import (
    IndustrialCanvasResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.cognite_file import CogniteFileResponse
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    ConstraintOrIndexState,
    DataModelResponseWithViews,
    DirectNodeRelation,
    EdgeRequest,
    EdgeResponse,
    FileCDFExternalIdReference,
    InstanceRequest,
    InstanceResponse,
    InstanceSource,
    Int64Property,
    MultiEdgeProperty,
    MultiReverseDirectRelationPropertyResponse,
    NodeRequest,
    NodeResponse,
    TextProperty,
    TimeseriesCDFExternalIdReference,
    TimestampProperty,
    ViewCorePropertyResponse,
    ViewResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.event import EventResponse
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataResponse
from cognite_toolkit._cdf_tk.client.resource_classes.migration import CreatedSourceSystem
from cognite_toolkit._cdf_tk.client.resource_classes.record_property_mapping import RecordPropertyMapping

from cognite_toolkit._cdf_tk.client.resource_classes.resource_view_mapping import ResourceViewMappingResponse
from cognite_toolkit._cdf_tk.client.resource_classes.three_d import (
    AssetMappingClassicResponse,
    AssetMappingDMRequestId,
)
from cognite_toolkit._cdf_tk.client.resource_classes.timeseries import TimeSeriesResponse
from cognite_toolkit._cdf_tk.client.resource_classes.view_to_view_mapping import ViewToViewMapping
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands._migrate.conversion import ConnectionCreator
from cognite_toolkit._cdf_tk.commands._migrate.data_classes import (
    AssetCentricMapping,
    MigrationMapping,
)
from cognite_toolkit._cdf_tk.commands._migrate.data_mapper import (
    AssetCentricToInstanceMapper,
    CanvasMapper,
    FDMtoCDMMapper,
    InFieldLegacyToCDMScheduleMapper,
    AssetCentricToRecordMapper,
    ThreeDAssetMapper,
)
from cognite_toolkit._cdf_tk.commands._migrate.issues import CanvasMigrationIssue
from cognite_toolkit._cdf_tk.commands._migrate.selectors import MigrationCSVFileSelector
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.storageio.logger import DataLogger, OperationTracker
from tests.data import MIGRATION_DIR


class TestAssetCentricToInstanceMapper:
    def test_map_assets(
        self,
        tmp_path: Path,
        cognite_core_no_3D: DataModelResponseWithViews,
        cognite_extractor_views: list[ViewResponse],
    ) -> None:
        asset_count = 10
        source = [
            AssetCentricMapping(
                mapping=MigrationMapping(
                    resource_type="asset",
                    instance_id=NodeId(space="my_space", external_id=f"asset_{i}"),
                    id=1000 + i,
                    ingestion_mapping="cdf_asset_mapping",
                ),
                resource=AssetResponse(
                    id=1000 + i,
                    name=f"Asset {i}",
                    source="SAP",
                    description=f"Description {i}",
                    created_time=1,
                    last_updated_time=1,
                    root_id=0,
                ),
            )
            for i in range(asset_count)
        ]

        mapping_file = tmp_path / "mapping.csv"
        mapping_file.write_text(
            "id,space,externalId,ingestionView\n"
            + "\n".join(f"{1000 + i},my_space,asset_{i},cdf_asset_mapping" for i in range(asset_count))
        )

        selected = MigrationCSVFileSelector(datafile=mapping_file, kind="Assets")

        with monkeypatch_toolkit_client() as client:
            client.migration.resource_view_mapping.retrieve.return_value = [
                ResourceViewMappingResponse(
                    external_id="cdf_asset_mapping",
                    resource_type="asset",
                    view_id=ViewId(space="cdf_cdm", external_id="CogniteAsset", version="v1"),
                    property_mapping={
                        "name": "name",
                        "description": "description",
                        "source": "source",
                    },
                    last_updated_time=1,
                    created_time=0,
                    version=1,
                )
            ]

            client.migration.created_source_system.retrieve.return_value = [
                CreatedSourceSystem(
                    space="source_systems",
                    external_id="SAP",
                    source="sap",
                    last_updated_time=1,
                    created_time=0,
                    version=1,
                ),
            ]
            client.tool.views.retrieve.return_value = cognite_core_no_3D.views + cognite_extractor_views

            mapper = AssetCentricToInstanceMapper(client)

            mapper.prepare(selected)

            mapped: list[InstanceApply] = []
            for target, item in zip(mapper.map(source), source):
                mapped.append(target)

            # We do not assert the exact content of mapped, as that is tested in the
            # tests for the asset_centric_to_dm function.
            assert len(mapped) == asset_count
            first_asset = mapped[0]
            assert first_asset.sources[0].properties["source"] == {"space": "source_systems", "externalId": "SAP"}

            # Check lookup calls
            assert client.migration.resource_view_mapping.retrieve.call_count == 1
            client.migration.resource_view_mapping.retrieve.assert_called_with(
                [NodeId(space="cognite_migration", external_id="cdf_asset_mapping")]
            )
            assert client.migration.created_source_system.retrieve.call_count == 1
            assert client.tool.views.retrieve.call_count == 1

            assert client.migration.created_source_system.retrieve.call_count == 1
            client.migration.created_source_system.retrieve.assert_called_with(["sap"])

    def test_map_chunk_before_prepare_raises_error(self, tmp_path: Path) -> None:
        """Test that calling map_chunk before prepare raises a RuntimeError."""
        source = AssetCentricMapping(
            mapping=MigrationMapping(
                resource_type="asset",
                instance_id=NodeId(space="my_space", external_id="asset_1"),
                id=1001,
                ingestion_mapping="cdf_asset_mapping",
            ),
            resource=AssetResponse(
                id=1001,
                name="Asset 1",
                description="Description 1",
                created_time=1,
                last_updated_time=0,
                root_id=0,
            ),
        )

        with monkeypatch_toolkit_client() as client:
            mapper = AssetCentricToInstanceMapper(client)

            # Call map_chunk without calling prepare first
            with pytest.raises(
                RuntimeError,
                match=r"Failed to lookup mapping or view for ingestion view 'cdf_asset_mapping'. Did you forget to call .prepare()?",
            ):
                mapper.map([source])

    def test_prepare_missing_view_source_raises_error(self, tmp_path: Path) -> None:
        """Test that prepare raises ToolkitValueError when view source is not found."""
        mapping_file = tmp_path / "mapping.csv"
        mapping_file.write_text("id,space,externalId,ingestionView\n1001,my_space,asset_1,missing_view_source")

        selected = MigrationCSVFileSelector(datafile=mapping_file, kind="Assets")

        with monkeypatch_toolkit_client() as client:
            # Return empty list to simulate missing view source
            client.migration.resource_view_mapping.retrieve.return_value = []

            mapper = AssetCentricToInstanceMapper(client)

            with pytest.raises(
                ToolkitValueError, match=r"The following ingestion views were not found: missing_view_source"
            ):
                mapper.prepare(selected)

    def test_prepare_missing_view_in_data_modeling_raises_error(self, tmp_path: Path) -> None:
        """Test that prepare raises ToolkitValueError when view is not found in Data Modeling."""
        mapping_file = tmp_path / "mapping.csv"
        mapping_file.write_text("id,space,externalId,ingestionView\n1001,my_space,asset_1,cdf_asset_mapping")

        selected = MigrationCSVFileSelector(datafile=mapping_file, kind="Assets")

        with monkeypatch_toolkit_client() as client:
            # Return view source but empty view list to simulate missing view in Data Modeling
            client.migration.resource_view_mapping.retrieve.return_value = [
                ResourceViewMappingResponse(
                    external_id="cdf_asset_mapping",
                    resource_type="asset",
                    view_id=ViewId(space="my_space", external_id="MyAsset", version="v1"),
                    property_mapping={
                        "name": "name",
                        "description": "description",
                    },
                    last_updated_time=1,
                    created_time=0,
                    version=1,
                )
            ]

            # Return empty list to simulate missing view in Data Modeling
            client.data_modeling.views.retrieve.return_value = []

            mapper = AssetCentricToInstanceMapper(client)

            with pytest.raises(ToolkitValueError) as exc_info:
                mapper.prepare(selected)

            assert "The following ingestion views were not found in Data Modeling" in str(exc_info.value)


class TestThreeDAssetMapper:
    DEFAULTS: ClassVar[dict[str, Any]] = {
        "modelId": 1,
        "revisionId": 1,
    }
    ASSET_ID = "AssetMapping_1"

    @pytest.mark.parametrize(
        "response,lookup_asset,expected",
        [
            pytest.param(
                AssetMappingClassicResponse(
                    nodeId=1234,
                    assetInstanceId=NodeId(space="my_space", externalId="asset_1"),
                    **DEFAULTS,
                ),
                None,
                AssetMappingDMRequestId(
                    nodeId=1234,
                    assetInstanceId=NodeId(space="my_space", externalId="asset_1"),
                    **DEFAULTS,
                ),
                id="Return existing assetInstanceId",
            ),
            pytest.param(
                AssetMappingClassicResponse(
                    nodeId=5678,
                    assetId=37,
                    **DEFAULTS,
                ),
                dm.NodeId(space="my_space", external_id="asset_2"),
                AssetMappingDMRequestId(
                    nodeId=5678,
                    assetInstanceId=NodeId(space="my_space", externalId="asset_2"),
                    **DEFAULTS,
                ),
                id="Lookup and return found assetInstanceId",
            ),
            pytest.param(
                AssetMappingClassicResponse(
                    nodeId=91011,
                    assetId=42,
                    **DEFAULTS,
                ),
                None,
                "Missing asset instance for asset ID 42",
                id="Lookup and return not found issue",
            ),
            pytest.param(
                AssetMappingClassicResponse(
                    nodeId=1213,
                    **DEFAULTS,
                ),
                None,
                "Neither assetInstanceId nor assetId provided for mapping.",
                id="Missing both assetInstanceId and assetId issue",
            ),
        ],
    )
    def test_map_chunk(
        self,
        response: AssetMappingClassicResponse,
        lookup_asset: dm.NodeId | None,
        expected: AssetMappingDMRequestId | str,
    ) -> None:
        with monkeypatch_toolkit_client() as client:
            client.migration.lookup.assets.return_value = lookup_asset

            mapper = ThreeDAssetMapper(client)
            logger = MagicMock(spec=DataLogger)
            logger.tracker = MagicMock(spec_set=OperationTracker)
            mapper.logger = logger
            mapped = mapper.map([response])[0]

            if lookup_asset is not None:
                # One for cache population, one for actual call
                assert client.migration.lookup.assets.call_count == 2
                last_call = client.migration.lookup.assets.call_args_list[-1]
                assert last_call.args == (response.asset_id,)

            if isinstance(expected, AssetMappingDMRequestId):
                logger.log.assert_not_called()
                logger.tracker.add_issue.assert_not_called()
                assert mapped is not None
                assert mapped.model_dump() == expected.model_dump()
            else:
                _, message = logger.tracker.add_issue.call_args.args
                assert mapped is None, "Expected no mapped result"
                assert message == expected


class TestCanvasMapper:
    def test_map_canvas_with_annotations(self):
        input_canvas_path = MIGRATION_DIR / "canvas" / "annotated_canvas.yaml"
        input_canvas = IndustrialCanvasResponse._load(yaml.safe_load(input_canvas_path.read_text(encoding="utf-8")))
        with monkeypatch_toolkit_client() as client:
            client.migration.lookup.assets.return_value = NodeId(space="my_space", external_id="asset_1")
            client.migration.lookup.events.return_value = NodeId(space="my_space", external_id="event_1")
            client.migration.lookup.files.side_effect = [
                {9005977951852492: NodeId(space="my_space", external_id="file_1")},
                NodeId(space="my_space", external_id="file_1"),
            ]
            client.migration.lookup.time_series.return_value = NodeId(space="my_space", external_id="timeseries_1")
            client.tool.cognite_files.retrieve.return_value = [
                CogniteFileResponse(
                    space="my_space",
                    external_id="file_1",
                    is_uploaded=False,
                    last_updated_time=1,
                    created_time=0,
                    version=37,
                ),
            ]
            client.migration.lookup.assets.consumer_view.return_value = ViewId(
                space="cdm_cdm", external_id="CogniteAsset", version="v1"
            )
            client.migration.lookup.events.consumer_view.return_value = ViewId(
                space="cdf_cdm", external_id="CogniteActivity", version="v1"
            )
            client.migration.lookup.files.consumer_view.return_value = ViewId(
                space="cdf_cdm", external_id="CogniteFile", version="v1"
            )
            client.migration.lookup.time_series.consumer_view.return_value = ViewId(
                space="cdf_cdm", external_id="CogniteTimeSeries", version="v1"
            )

            mapper = CanvasMapper(client, dry_run=False, skip_on_missing_ref=False)
            logger = MagicMock(spec=DataLogger)
            logger.tracker = MagicMock(spec_set=OperationTracker)
            mapper.logger = logger

            actual = mapper.map([input_canvas])[0]

        assert not actual.container_references
        assert len(actual.fdm_instance_container_references) == len(input_canvas.container_references)

        migrated_dumped_str = actual.dump_yaml()

        unexpected_uuid: list[str] = []
        for item in input_canvas.container_references:
            if item.id_ in migrated_dumped_str:
                unexpected_uuid.append(item.id_)
        # After the migration, there should be no references to the original components of the Canvas.
        assert not unexpected_uuid, f"Found unexpected user data in migrated canvas: {unexpected_uuid}"

        # Check log is written
        logger.log.assert_called_once()
        entry = logger.log.call_args[0][0]
        assert isinstance(entry, list)
        first = entry[0]
        assert isinstance(first, CanvasMigrationIssue)
        assert first.files_missing_content == [NodeId(space="my_space", external_id="file_1")]


class TestFDMtoCDMMapper:
    DEFAULT_ARGS: Mapping[str, Any] = dict(
        created_time=0,
        last_updated_time=1,
        writable=True,
        queryable=True,
        used_for="node",
        is_global=False,
        mapped_containers=[],
    )
    # Used for non-important references in the test dat.
    SOME_VIEW_ID = ViewId(space="schema_space1", external_id="SomeOtherView", version="v1")
    SOME_CONTAINER_ID = ContainerId(space="schema_space1", external_id="SomeContainer")
    SOURCE_VIEW = ViewResponse(
        space="schema_space1",
        external_id="SourceView",
        version="v1",
        **DEFAULT_ARGS,
        properties={
            "sourceEdge1": MultiEdgeProperty(
                type=NodeId(space="schema_space1", external_id="sourceEdge1"), source=SOME_VIEW_ID
            ),
            "sourceEdge2": MultiEdgeProperty(
                type=NodeId(space="schema_space1", external_id="sourceEdge2"), source=SOME_VIEW_ID
            ),
            "sourceEdge3": MultiEdgeProperty(
                type=NodeId(space="schema_space1", external_id="sourceEdge3"), source=SOME_VIEW_ID
            ),
            "timeseriesRef": ViewCorePropertyResponse(
                constraint_state=ConstraintOrIndexState(),
                type=TimeseriesCDFExternalIdReference(),
                container_property_identifier="timeseriesRef",
                container=SOME_CONTAINER_ID,
            ),
            "fileRef": ViewCorePropertyResponse(
                constraint_state=ConstraintOrIndexState(),
                type=FileCDFExternalIdReference(),
                container_property_identifier="fileRef",
                container=SOME_CONTAINER_ID,
            ),
            "textProp": ViewCorePropertyResponse(
                constraint_state=ConstraintOrIndexState(),
                type=TextProperty(),
                container_property_identifier="textProp",
                container=SOME_CONTAINER_ID,
            ),
        },
    )
    DESTINATION_VIEW = ViewResponse(
        space="schema_space2",
        external_id="DestinationView",
        version="v1",
        **DEFAULT_ARGS,
        properties={
            "targetEdge1": MultiEdgeProperty(
                type=NodeId(space="schema_space2", external_id="targetEdge1"), source=SOME_VIEW_ID
            ),
            "targetDirect1": ViewCorePropertyResponse(
                constraint_state=ConstraintOrIndexState(),
                type=DirectNodeRelation(),
                container_property_identifier="targetDirect1",
                container=SOME_CONTAINER_ID,
            ),
            "targetReverse1": MultiReverseDirectRelationPropertyResponse(
                source=SOME_VIEW_ID,
                through=ViewDirectId(source=SOME_VIEW_ID, identifier="someProp"),
                targets_list=True,
            ),
            "targetDirectTimeseries": ViewCorePropertyResponse(
                constraint_state=ConstraintOrIndexState(),
                type=DirectNodeRelation(),
                container_property_identifier="targetDirectTimeseries",
                container=SOME_CONTAINER_ID,
            ),
            "targetDirectFile": ViewCorePropertyResponse(
                constraint_state=ConstraintOrIndexState(),
                type=DirectNodeRelation(),
                container_property_identifier="targetDirectFile",
                container=SOME_CONTAINER_ID,
            ),
            "targetInt": ViewCorePropertyResponse(
                constraint_state=ConstraintOrIndexState(),
                type=Int64Property(),
                container_property_identifier="targetDirectText",
                container=SOME_CONTAINER_ID,
            ),
            "targetTimestamp": ViewCorePropertyResponse(
                constraint_state=ConstraintOrIndexState(),
                type=TimestampProperty(),
                container_property_identifier="targetTimestamp",
                container=SOME_CONTAINER_ID,
            ),
        },
    )
    SOURCE_VIEW_ID = SOURCE_VIEW.as_id()
    DESTINATION_VIEW_ID = DESTINATION_VIEW.as_id()
    SOURCE_SPACE = "source_space"
    TARGET_SPACE = "target_space"
    SPACE_MAPPING: Mapping[str, str] = {SOURCE_SPACE: TARGET_SPACE}
    VIEW_MAPPING = ViewToViewMapping(
        external_id="mapping_1",
        source_view=SOURCE_VIEW_ID,
        destination_view=DESTINATION_VIEW_ID,
        # Set by each test.
        container_mapping={},
    )

    FILE_RESPONSE = FileMetadataResponse(
        external_id="file1",
        name="file1",
        instance_id=NodeId(space=TARGET_SPACE, external_id="fileNode"),
        created_time=0,
        last_updated_time=0,
        uploaded=True,
        id=37,
    )
    TIMESERIES_RESPONSE = TimeSeriesResponse(
        external_id="timeseries1",
        instance_id=NodeId(space=TARGET_SPACE, external_id="timeseriesNode"),
        type="numeric",
        created_time=0,
        last_updated_time=0,
        id=42,
    )

    @pytest.mark.parametrize(
        "instances, container_mapping, edge_mapping, expected",
        [
            pytest.param(
                [
                    NodeResponse(
                        space=SOURCE_SPACE,
                        external_id="node1",
                        last_updated_time=1772522715000,
                        created_time=0,
                        version=1,
                        properties={SOURCE_VIEW_ID: {"textProp": "37"}},
                    )
                ],
                {
                    "textProp": "targetInt",
                    "node.lastUpdatedTime": "targetTimestamp",
                },
                None,
                [
                    NodeRequest(
                        space=TARGET_SPACE,
                        external_id="node1",
                        sources=[
                            InstanceSource(
                                source=DESTINATION_VIEW_ID,
                                properties={"targetTimestamp": "2026-03-03T07:25:15Z", "targetInt": 37},
                            )
                        ],
                    )
                ],
                id="Node.property mapped to container property and type change.",
            ),
            pytest.param(
                [
                    NodeResponse(
                        space=SOURCE_SPACE,
                        external_id="node1",
                        last_updated_time=1772522715000,
                        created_time=0,
                        version=1,
                        properties={
                            SOURCE_VIEW_ID: {
                                "timeseriesRef": TIMESERIES_RESPONSE.external_id,
                                "fileRef": FILE_RESPONSE.external_id,
                            }
                        },
                    )
                ],
                {
                    "timeseriesRef": "targetDirectTimeseries",
                    "fileRef": "targetDirectFile",
                },
                None,
                [
                    NodeRequest(
                        space=TARGET_SPACE,
                        external_id="node1",
                        sources=[
                            InstanceSource(
                                source=DESTINATION_VIEW_ID,
                                properties={
                                    "targetDirectTimeseries": TIMESERIES_RESPONSE.instance_id.dump(
                                        include_instance_type=False
                                    ),
                                    "targetDirectFile": FILE_RESPONSE.instance_id.dump(include_instance_type=False),
                                },
                            )
                        ],
                    )
                ],
                id="Timeseries/File reference to direct relations.",
            ),
            pytest.param(
                [
                    NodeResponse(
                        space=SOURCE_SPACE,
                        external_id="node1",
                        last_updated_time=1772522715000,
                        created_time=0,
                        version=1,
                        properties={
                            SOURCE_VIEW_ID: {
                                "textProp": "37",
                            }
                        },
                    ),
                    EdgeResponse(
                        space=SOURCE_SPACE,
                        external_id="edge1",
                        last_updated_time=1,
                        created_time=0,
                        version=1,
                        type=NodeId(space="schema_space1", external_id="sourceEdge1"),
                        start_node=NodeId(space=SOURCE_SPACE, external_id="node1"),
                        end_node=NodeId(space=SOURCE_SPACE, external_id="node2"),
                    ),
                    EdgeResponse(
                        space=SOURCE_SPACE,
                        external_id="edge2",
                        last_updated_time=2,
                        created_time=0,
                        version=1,
                        type=NodeId(space="schema_space1", external_id="sourceEdge2"),
                        start_node=NodeId(space=SOURCE_SPACE, external_id="node1"),
                        end_node=NodeId(space=SOURCE_SPACE, external_id="node3"),
                    ),
                    EdgeResponse(
                        space=SOURCE_SPACE,
                        external_id="edge3",
                        last_updated_time=3,
                        created_time=0,
                        version=1,
                        type=NodeId(space="schema_space1", external_id="sourceEdge3"),
                        start_node=NodeId(space=SOURCE_SPACE, external_id="node1"),
                        end_node=NodeId(space=SOURCE_SPACE, external_id="node4"),
                    ),
                ],
                {"textProp": "targetInt"},
                {
                    EdgeTypeId(
                        type=NodeId(space="schema_space1", external_id="sourceEdge1"), direction="outwards"
                    ): "targetEdge1",
                    EdgeTypeId(
                        type=NodeId(space="schema_space1", external_id="sourceEdge2"), direction="outwards"
                    ): "targetDirect1",
                    EdgeTypeId(
                        type=NodeId(space="schema_space1", external_id="sourceEdge3"), direction="outwards"
                    ): "targetReverse1",
                },
                [
                    NodeRequest(
                        space=TARGET_SPACE,
                        external_id="node1",
                        sources=[
                            InstanceSource(
                                source=DESTINATION_VIEW_ID,
                                properties={
                                    "targetInt": 37,
                                    "targetDirect1": {"space": TARGET_SPACE, "externalId": "node3"},
                                },
                            )
                        ],
                    ),
                    EdgeRequest(
                        space=TARGET_SPACE,
                        external_id="edge1",
                        type=NodeId(space="schema_space2", external_id="targetEdge1"),
                        start_node=NodeId(space=TARGET_SPACE, external_id="node1"),
                        end_node=NodeId(space=TARGET_SPACE, external_id="node2"),
                    ),
                ],
                id="Edges converted to edge, direct relations, and reverse direct relations (ignored).",
            ),
        ],
    )
    def test_map_fdm_to_cdm(
        self,
        instances: Sequence[InstanceResponse],
        container_mapping: dict[str, str],
        edge_mapping: dict[EdgeTypeId, str] | None,
        expected: Sequence[InstanceRequest],
    ):
        with monkeypatch_toolkit_client() as client:
            client.tool.views.retrieve.return_value = [self.SOURCE_VIEW, self.DESTINATION_VIEW]
            client.tool.timeseries.retrieve.return_value = [self.TIMESERIES_RESPONSE]
            client.tool.filemetadata.retrieve.return_value = [self.FILE_RESPONSE]

            mapping = self.VIEW_MAPPING.model_copy(
                update={"container_mapping": container_mapping, "edge_mapping": edge_mapping}
            )
            connection_creator = ConnectionCreator(client, space_mapping=self.SPACE_MAPPING)
            mapper = FDMtoCDMMapper(client, [mapping], connection_creator)
            mapper.prepare(MagicMock())

            actual = mapper.map(instances)
            assert [item.dump() for item in actual] == [item.dump() for item in expected]


class TestInFieldLegacyToCDMScheduleMapper:
    SOURCE_SPACE = "source_space"
    TARGET_SPACE = "target_space"
    SCHEDULE_VIEW = ViewId(space="cdf_apm", external_id="Schedule", version="v4")
    DEST_VIEW_ID = ViewId(
        space="infield_cdm_source_desc_sche_asset_file_ts",
        external_id="Schedule",
        version="v1",
    )
    CONTAINER_ID = ContainerId(space="infield_cdm_source_desc_sche_asset_file_ts", external_id="Schedule")
    TEMPLATE_EDGE_TYPE = NodeId(space="cdf_apm", external_id="referenceTemplateItems")
    TEMPLATE_ITEM_EDGE_TYPE = NodeId(space="cdf_apm", external_id="referenceSchedules")
    SPACE_MAPPING: Mapping[str, str] = {SOURCE_SPACE: TARGET_SPACE}

    GROUP_A_PROPS: ClassVar[dict[str, Any]] = {
        "until": "2026-01-01T00:00:00Z",
        "freq": "WEEKLY",
        "interval": "1",
        "timezone": "UTC",
        "status": "active",
        "startTime": "2025-01-01T08:00:00Z",
        "endTime": "2026-12-31T16:00:00Z",
    }
    GROUP_B_PROPS: ClassVar[dict[str, Any]] = {
        "until": "2027-06-01T00:00:00Z",
        "freq": "MONTHLY",
        "interval": "2",
        "timezone": "Europe/Oslo",
        "status": "active",
        "startTime": "2025-06-01T08:00:00Z",
        "endTime": "2027-05-31T16:00:00Z",
    }

    DEFAULT_VIEW_ARGS: ClassVar[dict[str, Any]] = dict(
        created_time=0,
        last_updated_time=1,
        writable=True,
        queryable=True,
        used_for="node",
        is_global=False,
        mapped_containers=[],
    )

    @pytest.fixture
    def schedule_instance_data(self) -> list[InstanceResponse]:
        """Creates a test case for InField Schedule mapping.

        The consist of 5 schedules, where they are 2 unique schedules (2 + 3).
        All schedules are connected to a simple template and several template item.
        """
        template = NodeId(space=self.SOURCE_SPACE, external_id="template_1")
        items = [NodeId(space=self.SOURCE_SPACE, external_id=f"item_{i}") for i in range(1, 6)]
        schedule_external_ids = ["schedule_a1", "schedule_a2", "schedule_b1", "schedule_b2", "schedule_b3"]
        schedule_props = [
            self.GROUP_A_PROPS,
            self.GROUP_A_PROPS,
            self.GROUP_B_PROPS,
            self.GROUP_B_PROPS,
            self.GROUP_B_PROPS,
        ]

        schedules: list[InstanceResponse] = [
            NodeResponse(
                space=self.SOURCE_SPACE,
                external_id=ext_id,
                created_time=100,
                last_updated_time=200,
                version=1,
                properties={self.SCHEDULE_VIEW: props.copy()},
            )
            for ext_id, props in zip(schedule_external_ids, schedule_props)
        ]

        template_to_item_edges: list[InstanceResponse] = [
            EdgeResponse(
                space=self.SOURCE_SPACE,
                external_id=f"edge_template_item_{i}",
                created_time=0,
                last_updated_time=0,
                version=1,
                type=self.TEMPLATE_EDGE_TYPE,
                start_node=template,
                end_node=items[i - 1],
            )
            for i in range(1, 6)
        ]

        item_to_schedule_edges: list[InstanceResponse] = [
            EdgeResponse(
                space=self.SOURCE_SPACE,
                external_id=f"edge_item_{ext_id}",
                created_time=0,
                last_updated_time=0,
                version=1,
                type=self.TEMPLATE_ITEM_EDGE_TYPE,
                start_node=items[i],
                end_node=NodeId(space=self.SOURCE_SPACE, external_id=ext_id),
            )
            for i, ext_id in enumerate(schedule_external_ids)
        ]

        return schedules + template_to_item_edges + item_to_schedule_edges

    def test_map(
        self,
        schedule_instance_data: list[InstanceResponse],
        data_regression: DataRegressionFixture,
    ) -> None:
        # Mock of the destination Schedlue view.
        dest_view = ViewResponse(
            space=self.DEST_VIEW_ID.space,
            external_id=self.DEST_VIEW_ID.external_id,
            version=self.DEST_VIEW_ID.version,
            **self.DEFAULT_VIEW_ARGS,
            properties={
                **{
                    prop: ViewCorePropertyResponse(
                        constraint_state=ConstraintOrIndexState(),
                        type=TextProperty(),
                        container_property_identifier=prop,
                        container=self.CONTAINER_ID,
                        nullable=True,
                    )
                    for prop in ["until", "freq", "interval", "timezone", "status", "startTime", "endTime"]
                },
                "template": ViewCorePropertyResponse(
                    constraint_state=ConstraintOrIndexState(),
                    type=DirectNodeRelation(),
                    container_property_identifier="template",
                    container=self.CONTAINER_ID,
                ),
                "templateItems": ViewCorePropertyResponse(
                    constraint_state=ConstraintOrIndexState(),
                    type=DirectNodeRelation(list=True),
                    container_property_identifier="templateItems",
                    container=self.CONTAINER_ID,
                ),
            },
        )

        mapping = ViewToViewMapping(
            external_id="InFieldScheduleMapping",
            source_view=self.SCHEDULE_VIEW,
            destination_view=self.DEST_VIEW_ID,
            map_identical_id_properties=True,
            container_mapping={
                "node.createdTime": "sourceCreatedTime",
                "node.lastUpdatedTime": "sourceUpdatedTime",
            },
        )

        with monkeypatch_toolkit_client() as client:
            client.tool.views.retrieve.return_value = [dest_view]

            connection_creator = ConnectionCreator(client, space_mapping=self.SPACE_MAPPING)
            mapper = InFieldLegacyToCDMScheduleMapper(client, connection_creator, mapping)
            mapper.prepare(MagicMock())

            result = mapper.map(schedule_instance_data)

        mapped_schedules = [r for r in result if r is not None]
        assert len(mapped_schedules) == 2

        data_regression.check({"schedules": [s.dump() for s in mapped_schedules]})


class TestAssetCentricToRecordMapper:
    CONTAINER_ID = ContainerId(space="stream_space", external_id="EventContainer")
    STREAM_EXTERNAL_ID = "my_stream"

    def _make_record_mapping(self, property_mapping: dict[str, str] | None = None) -> RecordPropertyMapping:
        return RecordPropertyMapping(
            external_id="my_event_mapping",
            resource_type="event",
            container_id=self.CONTAINER_ID,
            stream_external_id=self.STREAM_EXTERNAL_ID,
            property_mapping={"description": "description"} if property_mapping is None else property_mapping,
        )

    def _make_container_response(self) -> Any:
        container = MagicMock()
        container.as_id.return_value = self.CONTAINER_ID
        container.properties = {
            "description": ContainerPropertyDefinition(type=TextProperty(), nullable=True, immutable=False, auto_increment=False),
        }
        return container

    def _make_source(self, index: int, ingestion_mapping: str = "my_event_mapping") -> AssetCentricMapping:
        return AssetCentricMapping(
            mapping=MigrationMapping(
                resource_type="event",
                instance_id=NodeId(space="my_space", external_id=f"event_{index}"),
                id=index,
                ingestion_mapping=ingestion_mapping,
            ),
            resource=EventResponse(
                id=index,
                description=f"Event {index}",
                created_time=0,
                last_updated_time=1,
            ),
        )

    def test_map_no_properties_yields_none(self) -> None:
        """If no properties map successfully the record must be None and an issue logged."""
        source = [self._make_source(0)]
        record_mapping = self._make_record_mapping(property_mapping={})  # nothing to map

        with monkeypatch_toolkit_client() as client:
            client.tool.containers.retrieve.return_value = [self._make_container_response()]

            mapper = AssetCentricToRecordMapper(client, record_mappings=[record_mapping])
            mapper.logger = MagicMock(spec=DataLogger)
            mapper.logger.tracker = MagicMock(spec_set=OperationTracker)
            mapper.logger.log = MagicMock()
            mapper.prepare(MagicMock())
            results = mapper.map(source)

        assert results[0] is None
        issue_messages = [call.args[1] for call in mapper.logger.tracker.add_issue.call_args_list]
        assert any("No properties could be successfully mapped" in msg for msg in issue_messages)

    def test_prepare_missing_container_raises(self) -> None:
        record_mapping = self._make_record_mapping()

        with monkeypatch_toolkit_client() as client:
            client.tool.containers.retrieve.return_value = []

            mapper = AssetCentricToRecordMapper(client, record_mappings=[record_mapping])
            with pytest.raises(ToolkitValueError, match="not found in CDF"):
                mapper.prepare(MagicMock())
