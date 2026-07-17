import math
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, ClassVar
from unittest.mock import MagicMock

import pytest
import yaml
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling import InstanceApply
from pytest_regressions.data_regression import DataRegressionFixture

from cognite_toolkit._cdf_tk.client.identifiers import ContainerId, EdgeTypeId, InternalId, NodeId, ViewDirectId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.annotation import (
    AnnotationGeometry,
    AnnotationPoint,
    AnnotationPolygon,
    AnnotationResponse,
    BoundingBox,
    ImageAssetLinkData,
)
from cognite_toolkit._cdf_tk.client.resource_classes.asset import AssetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.canvas import (
    IndustrialCanvasResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.chart import ChartRequest, ChartResponse
from cognite_toolkit._cdf_tk.client.resource_classes.chart_monitoring_job import (
    ChartMonitoringJobModel,
    ChartMonitoringJobResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.chart_scheduled_calculation import (
    CalculationGraph,
    CalculationInput,
    CalculationStep,
    ChartScheduledCalculationResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.charts_data import ChartData
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
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling._container import (
    ContainerPropertyDefinition,
    ContainerResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.event import EventResponse
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataResponse
from cognite_toolkit._cdf_tk.client.resource_classes.migration import CreatedSourceSystem
from cognite_toolkit._cdf_tk.client.resource_classes.record_property_mapping import RecordPropertyMapping
from cognite_toolkit._cdf_tk.client.resource_classes.resource_view_mapping import ResourceViewMappingResponse
from cognite_toolkit._cdf_tk.client.resource_classes.three_d import (
    AssetMappingClassicResponse,
    AssetMappingDMRequestId,
    ThreeDModelClassicResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.timeseries import TimeSeriesResponse
from cognite_toolkit._cdf_tk.client.resource_classes.view_to_view_mapping import ViewToViewMapping
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands._migrate.conversion import (
    ConnectionCreator,
    SpaceMappingInstanceIdMapper,
    SuffixInstanceIdMapper,
)
from cognite_toolkit._cdf_tk.commands._migrate.data_classes import (
    AssetCentricMapping,
    AssetMapping,
    EventMapping,
    MigrationMapping,
)
from cognite_toolkit._cdf_tk.commands._migrate.data_mapper import (
    AssetCentricToInstanceMapper,
    AssetCentricToRecordMapper,
    CanvasMapper,
    ChartMapper,
    FDMtoCDMMapper,
    Image360AnnotationMapper,
    Image360CollectionMapper,
    Image360FDMtoCDMMapper,
    InFieldLegacyToCDMScheduleMapper,
    Station360PropertiesMapping,
    ThreeDAssetMapper,
)
from cognite_toolkit._cdf_tk.commands._migrate.image_360_mappings import (
    COGNITE_3D_REVISION_VIEW,
    COGNITE_360_IMAGE_VIEW,
    LEGACY_360_IMAGE_SCHEMA_SPACE,
    LEGACY_IMAGE360_COLLECTION_SOURCE_VIEW,
    LEGACY_IMAGE360_SOURCE_VIEW,
    LEGACY_IMAGE360_STATION_SOURCE_VIEW,
    create_360_image_selectors,
)
from cognite_toolkit._cdf_tk.commands._migrate.issues import MigrationEntryV2
from cognite_toolkit._cdf_tk.commands._migrate.selectors import Image360AnnotationSelector, MigrationCSVFileSelector
from cognite_toolkit._cdf_tk.dataio import DataItem
from cognite_toolkit._cdf_tk.dataio.logger import DataLogger, FileWithAggregationLogger, Severity
from cognite_toolkit._cdf_tk.dataio.selectors import InstanceQuerySelector, InstanceViewSelector
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.utils.text import sanitize_instance_external_id
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
            for data_item in mapper.map([DataItem(tracking_id=str(i), item=s) for i, s in enumerate(source)]):
                mapped.append(data_item.item)

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
                mapper.map([DataItem(tracking_id="t", item=source)])

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
            mapper.logger = logger
            result = mapper.map([DataItem(tracking_id="t", item=response)])
            mapped = result[0].item if result else None
            if lookup_asset is not None:
                # One for cache population, one for actual call
                assert client.migration.lookup.assets.call_count == 2
                last_call = client.migration.lookup.assets.call_args_list[-1]
                assert last_call.args == (response.asset_id,)

            if isinstance(expected, AssetMappingDMRequestId):
                logger.log.assert_not_called()
                assert mapped is not None
                assert mapped.model_dump() == expected.model_dump()
            else:
                logger.log.assert_called_once()
                log_entries = logger.log.call_args[0][0]
                assert isinstance(log_entries, list)
                assert len(log_entries) == 1
                entry = log_entries[0]
                assert isinstance(entry, MigrationEntryV2)
                assert mapped is None, "Expected no mapped result"
                assert expected in entry.message


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
            mapper.logger = logger

            actual = mapper.map([DataItem(tracking_id="t", item=input_canvas)])[0].item

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
        assert isinstance(first, MigrationEntryV2)
        assert first.label == "File missing content"
        assert "my_space:file_1" in first.attributes


class TestChartMapper:
    def test_map_chart(self, data_regression: DataRegressionFixture) -> None:
        input_chart_path = MIGRATION_DIR / "charts" / "classic.Chart.yaml"
        output_chart_path = MIGRATION_DIR / "charts" / "dms.Chart.yaml"
        target_space = "my_target_space"
        event_count = 4
        raw = yaml.safe_load(input_chart_path.read_text(encoding="utf-8"))
        source = ChartResponse.model_validate(raw)
        with monkeypatch_toolkit_client() as client:
            time_series_lookup = MagicMock()

            def _get_node_id(
                id: int | Sequence[int] | None = None, external_id: str | Sequence[str] | None = None
            ) -> dict[int, NodeId] | dict[str, NodeId] | NodeId | None:
                if isinstance(external_id, str):
                    return NodeId(space=target_space, external_id=external_id)
                return None

            time_series_lookup.side_effect = _get_node_id
            # Assume all timeseries in the output chart are set to CogniteTimeSeries
            time_series_lookup.consumer_view.return_value = ViewId(
                space="cdf_cdm", external_id="CogniteTimeSeries", version="v1"
            )
            client.migration.lookup.time_series = time_series_lookup
            assert source.monitoring_jobs

            event_node_ids = [NodeId(space=target_space, external_id=f"event_{i}") for i in range(event_count)]
            client.tool.events.list.return_value = [
                EventResponse(id=i, created_time=1, last_updated_time=1) for i in range(event_count)
            ]
            event_lookup = MagicMock()
            # Cache call + each event
            event_lookup.side_effect = [None, *event_node_ids]
            # Assume all events in the output chart are set to CogniteActivity
            event_lookup.consumer_view.return_value = ViewId(
                space="cdf_cdm", external_id="CogniteActivity", version="v1"
            )
            client.migration.lookup.events = event_lookup

            mapper = ChartMapper(client)
            mapped_list = mapper.map([DataItem(tracking_id="t", item=source)])
            assert len(mapped_list) == 1
            mapped = mapped_list[0].item
            assert isinstance(mapped, ChartRequest)

        dumped = mapped.dump()
        dumped["monitoringJobs"] = [job.dump() for job in mapped.monitoring_jobs or []] or None
        dumped["scheduledCalculations"] = [
            calculation.dump() for calculation in mapped.scheduled_calculations or []
        ] or None

        data_regression.check(dumped, fullpath=output_chart_path)

    @pytest.mark.parametrize(
        "scheduled_calculations, monitoring_jobs",
        [
            pytest.param(
                [
                    ChartScheduledCalculationResponse(
                        external_id="calc_1",
                        period=60_000,
                        target_timeseries_external_id="OLD_TARGET_TS",
                        graph=CalculationGraph(granularity="1m", steps=[]),
                        created_time=0,
                        last_updated_time=0,
                    )
                ],
                None,
                id="legacy-calc-target",
            ),
            pytest.param(
                [
                    ChartScheduledCalculationResponse(
                        external_id="calc_2",
                        period=60_000,
                        graph=CalculationGraph(
                            granularity="1m",
                            steps=[
                                CalculationStep(
                                    op="PASSTHROUGH",
                                    version=1.0,
                                    inputs=[CalculationInput(type="ts", value="OLD_INPUT_TS")],
                                    raw=False,
                                    step=0,
                                )
                            ],
                        ),
                        created_time=0,
                        last_updated_time=0,
                    )
                ],
                None,
                id="legacy-calc-graph-input",
            ),
            pytest.param(
                None,
                [
                    ChartMonitoringJobResponse(
                        external_id="job_1",
                        name="My Job",
                        channel_id=1,
                        model=ChartMonitoringJobModel(timeseries_external_id="OLD_MONITORING_TS"),
                        id=1,
                        interval=60_000,
                        overlap=0,
                    )
                ],
                id="legacy-monitoring-job-external-id",
            ),
            pytest.param(
                None,
                [
                    ChartMonitoringJobResponse(
                        external_id="job_2",
                        name="My Job",
                        channel_id=1,
                        model=ChartMonitoringJobModel(timeseries_id=42),
                        id=2,
                        interval=60_000,
                        overlap=0,
                    )
                ],
                id="legacy-monitoring-job-internal-id",
            ),
        ],
    )
    def test_has_legacy_backend_refs_detects_unmigrated_references(
        self,
        scheduled_calculations: list[ChartScheduledCalculationResponse] | None,
        monitoring_jobs: list[ChartMonitoringJobResponse] | None,
    ) -> None:
        chart = ChartResponse(
            external_id="chart_partial_migration",
            visibility="PUBLIC",
            created_time=0,
            last_updated_time=0,
            owner_id="user@example.com",
            data=ChartData(
                version=1,
                name="Partially migrated chart",
                date_from="2024-01-01T00:00:00Z",
                date_to="2024-12-31T00:00:00Z",
                time_series_collection=None,
                core_timeseries_collection=[],
            ),
            scheduled_calculations=scheduled_calculations,
            monitoring_jobs=monitoring_jobs,
        )

        assert ChartMapper._has_legacy_backend_refs(chart)

    def test_skip_dms_chart(self, tmp_path: Path) -> None:
        dms_chart = MIGRATION_DIR / "charts" / "dms.Chart.yaml"
        request_format = yaml.safe_load(dms_chart.read_text(encoding="utf-8"))
        # Make this into response
        request_format["createdTime"] = 0
        request_format["lastUpdatedTime"] = 0
        request_format["ownerId"] = "me"
        request_format["monitoringJobs"][0]["id"] = -1

        chart = ChartResponse.model_validate(request_format)

        with monkeypatch_toolkit_client() as client:
            mapper = ChartMapper(client)
            logger = FileWithAggregationLogger(MagicMock())
            logger.register([chart.external_id])
            mapper.logger = logger
            result = mapper.map([DataItem(tracking_id="t", item=chart)])

        assert result == []

        aggregations = logger.aggregations_by_ids[chart.external_id]
        assert len(aggregations) == 1
        assert aggregations[0].severity == Severity.skipped


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
            connection_creator = ConnectionCreator(
                client, instance_id_mapper=SpaceMappingInstanceIdMapper(self.SPACE_MAPPING)
            )
            mapper = FDMtoCDMMapper(client, [mapping], connection_creator)
            mapper.prepare(MagicMock())

            actual = mapper.map([DataItem(tracking_id=str(i), item=inst) for i, inst in enumerate(instances)])
            assert [data_item.item.dump() for data_item in actual] == [item.dump() for item in expected]

    def test_map_emits_all_mapped_items_with_tracking_ids(self) -> None:
        node = NodeResponse(
            space=self.SOURCE_SPACE,
            external_id="node1",
            last_updated_time=1772522715000,
            created_time=0,
            version=1,
            properties={self.SOURCE_VIEW_ID: {"textProp": "37"}},
        )
        edge = EdgeResponse(
            space=self.SOURCE_SPACE,
            external_id="edge1",
            last_updated_time=1,
            created_time=0,
            version=1,
            type=NodeId(space="schema_space1", external_id="sourceEdge1"),
            start_node=NodeId(space=self.SOURCE_SPACE, external_id="node1"),
            end_node=NodeId(space=self.SOURCE_SPACE, external_id="node2"),
        )
        with monkeypatch_toolkit_client() as client:
            client.tool.views.retrieve.return_value = [self.SOURCE_VIEW, self.DESTINATION_VIEW]
            mapping = self.VIEW_MAPPING.model_copy(
                update={
                    "container_mapping": {"textProp": "targetInt"},
                    "edge_mapping": {
                        EdgeTypeId(
                            type=NodeId(space="schema_space1", external_id="sourceEdge1"), direction="outwards"
                        ): "targetEdge1",
                    },
                }
            )
            connection_creator = ConnectionCreator(
                client, instance_id_mapper=SpaceMappingInstanceIdMapper(self.SPACE_MAPPING)
            )
            mapper = FDMtoCDMMapper(client, [mapping], connection_creator)
            mapper.prepare(MagicMock())

            source_items = [
                DataItem(tracking_id=f"{self.SOURCE_SPACE}:node1", item=node),
                DataItem(tracking_id=f"{self.SOURCE_SPACE}:edge1", item=edge),
            ]
            mapped_items = mapper.map(source_items)

        assert len(mapped_items) == 2
        assert mapped_items[0].tracking_id == f"{self.SOURCE_SPACE}:node1"
        assert isinstance(mapped_items[0].item, NodeRequest)
        assert mapped_items[1].tracking_id == f"{self.SOURCE_SPACE}:node1"
        assert isinstance(mapped_items[1].item, EdgeRequest)

    @pytest.mark.parametrize(
        "dry_run, expected_log_calls",
        [
            pytest.param(True, 0, id="dry-run treats mapped nodes as existing"),
            pytest.param(False, 1, id="real run surfaces missing target error"),
        ],
    )
    def test_dry_run_treats_mapped_nodes_as_existing(self, dry_run: bool, expected_log_calls: int) -> None:
        """In a real run, a direct relation pointing at a node mapped in a previous step is fine because that
        node has already been uploaded. In a dry-run, nothing is uploaded, so without special handling
        the constraint check would falsely flag the target as missing. The mapper pre-populates its
        existing-node cache for just-mapped nodes when ``dry_run`` is True.
        """
        constrained_container = ContainerId(space="schema_space2", external_id="ConstrainedContainer")
        source_view = self.SOURCE_VIEW.model_copy(deep=True)
        source_view.properties["sourceDirect"] = ViewCorePropertyResponse(
            constraint_state=ConstraintOrIndexState(),
            type=DirectNodeRelation(),
            container_property_identifier="sourceDirect",
            container=self.SOME_CONTAINER_ID,
        )
        destination_view = self.DESTINATION_VIEW.model_copy(deep=True)
        destination_view.properties["constrainedDirect"] = ViewCorePropertyResponse(
            constraint_state=ConstraintOrIndexState(),
            type=DirectNodeRelation(container=constrained_container),
            container_property_identifier="constrainedDirect",
            container=self.SOME_CONTAINER_ID,
        )

        with monkeypatch_toolkit_client() as client:
            client.tool.views.retrieve.return_value = [source_view, destination_view]
            # The target node referenced by the direct relation does not yet exist in CDF.
            client.tool.instances.retrieve.return_value = []

            mapping = self.VIEW_MAPPING.model_copy(update={"container_mapping": {"sourceDirect": "constrainedDirect"}})
            connection_creator = ConnectionCreator(
                client, instance_id_mapper=SpaceMappingInstanceIdMapper(self.SPACE_MAPPING)
            )
            mapper = FDMtoCDMMapper(client, [mapping], connection_creator)
            mapper.dry_run = dry_run
            logger = MagicMock(spec=DataLogger)
            mapper.logger = logger
            mapper.prepare(MagicMock())

            # First step: map the would-be target of the direct relation.
            mapper.map(
                [
                    DataItem(
                        tracking_id="t",
                        item=NodeResponse(
                            space=self.SOURCE_SPACE,
                            external_id="first",
                            last_updated_time=1,
                            created_time=0,
                            version=1,
                            properties={self.SOURCE_VIEW_ID: {}},
                        ),
                    )
                ]
            )
            logger.reset_mock()

            # Second step: map a node whose direct relation points at "first".
            mapper.map(
                [
                    DataItem(
                        tracking_id="t",
                        item=NodeResponse(
                            space=self.SOURCE_SPACE,
                            external_id="second",
                            last_updated_time=1,
                            created_time=0,
                            version=1,
                            properties={
                                self.SOURCE_VIEW_ID: {
                                    "sourceDirect": {"space": self.SOURCE_SPACE, "externalId": "first"}
                                }
                            },
                        ),
                    )
                ]
            )

        assert logger.log.call_count == expected_log_calls
        if expected_log_calls:
            entries = logger.log.call_args[0][0]
            assert len(entries) == 1
            entry = entries[0]
            assert isinstance(entry, MigrationEntryV2)
            assert entry.id == f"{self.SOURCE_SPACE}:second"
            assert "does not exist" in entry.message

    @classmethod
    def _make_image360_views(cls, cube_map_properties: dict[str, str]) -> tuple[ViewResponse, ViewResponse]:
        image360_container = ContainerId(space=LEGACY_360_IMAGE_SCHEMA_SPACE, external_id="Image360")
        cognite360_container = ContainerId(space="cdf_cdm", external_id="Cognite360Image")
        source_view = ViewResponse(
            space=LEGACY_IMAGE360_SOURCE_VIEW.space,
            external_id=LEGACY_IMAGE360_SOURCE_VIEW.external_id,
            version=LEGACY_IMAGE360_SOURCE_VIEW.version,
            **cls.DEFAULT_ARGS,
            properties={
                prop_id: ViewCorePropertyResponse(
                    constraint_state=ConstraintOrIndexState(),
                    type=FileCDFExternalIdReference(),
                    container_property_identifier=prop_id,
                    container=image360_container,
                )
                for prop_id in cube_map_properties
            },
        )
        destination_view = ViewResponse(
            space=COGNITE_360_IMAGE_VIEW.space,
            external_id=COGNITE_360_IMAGE_VIEW.external_id,
            version=COGNITE_360_IMAGE_VIEW.version,
            **cls.DEFAULT_ARGS,
            properties={
                dest_prop_id: ViewCorePropertyResponse(
                    constraint_state=ConstraintOrIndexState(),
                    type=DirectNodeRelation(),
                    container_property_identifier=dest_prop_id,
                    container=cognite360_container,
                )
                for dest_prop_id in ("front", "back", "left", "right", "top", "bottom")
            },
        )
        return source_view, destination_view

    @pytest.mark.parametrize(
        "available_file_count,expect_failure",
        [
            pytest.param(0, True, id="no_files_migrated"),
            pytest.param(4, True, id="partial_files_migrated"),
            pytest.param(6, False, id="all_files_migrated"),
        ],
    )
    def test_image360_cubemap_guard(self, available_file_count: int, expect_failure: bool) -> None:
        """Image360 nodes missing any cubemap face file must be rejected entirely, including partial migration."""
        face_file_ids = [f"face-{index}" for index in range(6)]
        cube_map_properties = dict(
            zip(
                ["cubeMapFront", "cubeMapBack", "cubeMapLeft", "cubeMapRight", "cubeMapTop", "cubeMapBottom"],
                face_file_ids,
                strict=True,
            )
        )
        all_file_responses = [
            FileMetadataResponse(
                external_id=external_id,
                name=external_id,
                instance_id=NodeId(space=self.TARGET_SPACE, external_id=f"file-{external_id}"),
                created_time=0,
                last_updated_time=0,
                uploaded=True,
                id=index,
            )
            for index, external_id in enumerate(face_file_ids)
        ]
        source_view, destination_view = self._make_image360_views(cube_map_properties)
        node = NodeResponse(
            space=self.SOURCE_SPACE,
            external_id="image1",
            last_updated_time=1,
            created_time=0,
            version=1,
            properties={LEGACY_IMAGE360_SOURCE_VIEW: cube_map_properties},
        )

        with monkeypatch_toolkit_client() as client:
            client.tool.views.retrieve.return_value = [source_view, destination_view]
            client.tool.filemetadata.retrieve.return_value = all_file_responses[:available_file_count]
            client.tool.instances.retrieve.return_value = []

            connection_creator = ConnectionCreator(client, instance_id_mapper=SuffixInstanceIdMapper())
            mapper = Image360FDMtoCDMMapper(client, connection_creator=connection_creator)
            logger = MagicMock(spec=DataLogger)
            mapper.logger = logger
            mapper.prepare(MagicMock())

            actual = mapper.map([DataItem(tracking_id=f"{self.SOURCE_SPACE}:image1", item=node)])

        if expect_failure:
            assert actual == []
            logger.log.assert_called_once()
            entry = logger.log.call_args[0][0][0]
            assert isinstance(entry, MigrationEntryV2)
            assert entry.severity == Severity.failure
            assert "have not been migrated" in entry.message
            for face_file_id in face_file_ids[available_file_count:]:
                assert face_file_id in entry.message
            assert f"{self.SOURCE_SPACE}:image1" == entry.id
        else:
            assert len(actual) == 1
            assert isinstance(actual[0].item, NodeRequest)
            assert actual[0].item.space == self.SOURCE_SPACE
            assert actual[0].item.external_id == sanitize_instance_external_id("image1", "_cdm")
            logger.log.assert_not_called()

    def test_image360_collection_mapper_leaves_model3d_unset_when_no_existing_model(self) -> None:
        collection_node = NodeResponse(
            space=self.SOURCE_SPACE,
            external_id="collection1",
            last_updated_time=1,
            created_time=0,
            version=1,
            properties={LEGACY_IMAGE360_COLLECTION_SOURCE_VIEW: {"label": "My collection"}},
        )

        with monkeypatch_toolkit_client() as client:
            client.tool.instances.retrieve.return_value = []
            mapper = Image360CollectionMapper(client)
            actual = mapper.map([DataItem(tracking_id=f"{self.SOURCE_SPACE}:collection1", item=collection_node)])

        client.tool.instances.create.assert_not_called()
        client.tool.three_d.models_classic.create.assert_not_called()

        assert len(actual) == 1
        collection_request = actual[0].item
        assert isinstance(collection_request, NodeRequest)
        assert collection_request.space == self.SOURCE_SPACE
        assert collection_request.external_id == sanitize_instance_external_id("collection1", "_cdm")
        model_source = next(
            source for source in collection_request.sources or [] if source.source.external_id == "Cognite3DRevision"
        )
        assert model_source.properties is not None
        assert "model3D" not in model_source.properties

    def test_image360_collection_mapper_reuses_existing_model3d_without_creating(self) -> None:
        collection_node = NodeResponse(
            space=self.SOURCE_SPACE,
            external_id="collection1",
            last_updated_time=1,
            created_time=0,
            version=1,
            properties={LEGACY_IMAGE360_COLLECTION_SOURCE_VIEW: {"label": "My collection"}},
        )
        migrated_external_id = sanitize_instance_external_id("collection1", "_cdm")
        existing_model_external_id = "cog_3d_model_99"
        migrated_node = NodeResponse(
            space=self.SOURCE_SPACE,
            external_id=migrated_external_id,
            last_updated_time=1,
            created_time=0,
            version=1,
            properties={
                COGNITE_3D_REVISION_VIEW: {
                    "model3D": {"space": self.SOURCE_SPACE, "externalId": existing_model_external_id},
                }
            },
        )

        with monkeypatch_toolkit_client() as client:
            client.tool.instances.retrieve.return_value = [migrated_node]
            mapper = Image360CollectionMapper(client)
            actual = mapper.map([DataItem(tracking_id=f"{self.SOURCE_SPACE}:collection1", item=collection_node)])

        client.tool.three_d.models_classic.create.assert_not_called()
        collection_request = actual[0].item
        assert isinstance(collection_request, NodeRequest)
        model_source = next(
            source for source in collection_request.sources or [] if source.source.external_id == "Cognite3DRevision"
        )
        assert model_source.properties is not None
        assert model_source.properties["model3D"] == {
            "space": self.SOURCE_SPACE,
            "externalId": existing_model_external_id,
        }

    def test_image360_mapper_maps_each_view_in_isolation(self) -> None:
        face_file_ids = [f"face-{index}" for index in range(6)]
        cube_map_properties = dict(
            zip(
                ["cubeMapFront", "cubeMapBack", "cubeMapLeft", "cubeMapRight", "cubeMapTop", "cubeMapBottom"],
                face_file_ids,
                strict=True,
            )
        )
        file_responses = [
            FileMetadataResponse(
                external_id=external_id,
                name=external_id,
                instance_id=NodeId(space=self.TARGET_SPACE, external_id=f"file-{external_id}"),
                created_time=0,
                last_updated_time=0,
                uploaded=True,
                id=index,
            )
            for index, external_id in enumerate(face_file_ids)
        ]
        image_source_view, image_destination_view = self._make_image360_views(cube_map_properties)
        station_source_container = ContainerId(space=LEGACY_360_IMAGE_SCHEMA_SPACE, external_id="Station360")
        station_destination_container = ContainerId(space="cdf_cdm", external_id="Cognite360ImageStation")
        group_container = ContainerId(space="cdf_cdm", external_id="Cognite3DGroup")
        station_source_view = ViewResponse(
            space=LEGACY_IMAGE360_STATION_SOURCE_VIEW.space,
            external_id=LEGACY_IMAGE360_STATION_SOURCE_VIEW.external_id,
            version=LEGACY_IMAGE360_STATION_SOURCE_VIEW.version,
            **self.DEFAULT_ARGS,
            properties={
                "label": ViewCorePropertyResponse(
                    constraint_state=ConstraintOrIndexState(),
                    type=TextProperty(),
                    container_property_identifier="label",
                    container=station_source_container,
                )
            },
        )
        station_destination_view = ViewResponse(
            space="cdf_cdm",
            external_id="Cognite360ImageStation",
            version="v1",
            **self.DEFAULT_ARGS,
            properties={
                "name": ViewCorePropertyResponse(
                    constraint_state=ConstraintOrIndexState(),
                    type=TextProperty(),
                    container_property_identifier="name",
                    container=station_destination_container,
                ),
                "groupType": ViewCorePropertyResponse(
                    constraint_state=ConstraintOrIndexState(),
                    type=TextProperty(),
                    container_property_identifier="groupType",
                    container=group_container,
                ),
            },
        )
        collection_node = NodeResponse(
            space=self.SOURCE_SPACE,
            external_id="collection1",
            last_updated_time=1,
            created_time=0,
            version=1,
            properties={LEGACY_IMAGE360_COLLECTION_SOURCE_VIEW: {"label": "My collection"}},
        )
        image_node = NodeResponse(
            space=self.SOURCE_SPACE,
            external_id="image1",
            last_updated_time=1,
            created_time=0,
            version=1,
            properties={LEGACY_IMAGE360_SOURCE_VIEW: cube_map_properties},
        )
        station_node = NodeResponse(
            space=self.SOURCE_SPACE,
            external_id="station1",
            last_updated_time=1,
            created_time=0,
            version=1,
            properties={LEGACY_IMAGE360_STATION_SOURCE_VIEW: {"label": "Station A"}},
        )
        created_model = ThreeDModelClassicResponse(
            id=42,
            name="My collection",
            created_time=0,
        )

        with monkeypatch_toolkit_client() as client:
            client.tool.views.retrieve.return_value = [
                image_source_view,
                image_destination_view,
                station_source_view,
                station_destination_view,
            ]
            client.tool.filemetadata.retrieve.return_value = file_responses
            client.tool.instances.retrieve.return_value = []
            client.tool.three_d.models_classic.create.return_value = [created_model]

            connection_creator = ConnectionCreator(client, instance_id_mapper=SuffixInstanceIdMapper())
            mapper = Image360FDMtoCDMMapper(
                client,
                connection_creator=connection_creator,
                custom_properties_mappings=[Station360PropertiesMapping()],
                custom_instance_mappings={
                    LEGACY_IMAGE360_COLLECTION_SOURCE_VIEW: Image360CollectionMapper(client),
                },
            )
            mapper.prepare(MagicMock())

            collection_results = mapper.map(
                [DataItem(tracking_id=f"{self.SOURCE_SPACE}:collection1", item=collection_node)]
            )
            station_results = mapper.map([DataItem(tracking_id=f"{self.SOURCE_SPACE}:station1", item=station_node)])
            image_results = mapper.map([DataItem(tracking_id=f"{self.SOURCE_SPACE}:image1", item=image_node)])

        assert len(collection_results) == 1
        assert len(station_results) == 1
        assert len(image_results) == 1

        collection_request = collection_results[0].item
        station_request = station_results[0].item
        image_request = image_results[0].item
        assert isinstance(collection_request, NodeRequest)
        assert isinstance(station_request, NodeRequest)
        assert isinstance(image_request, NodeRequest)

        assert collection_request.external_id == sanitize_instance_external_id("collection1", "_cdm")
        assert image_request.external_id == sanitize_instance_external_id("image1", "_cdm")
        assert station_request.external_id == sanitize_instance_external_id("station1", "_cdm")
        station_source = next(
            source for source in station_request.sources or [] if source.source.external_id == "Cognite360ImageStation"
        )
        assert station_source.properties == {"name": "Station A", "groupType": "Station360"}


def test_create_360_image_selectors_returns_collection_station_and_image_steps() -> None:
    collections = [
        NodeId(space="img_space", external_id="col1"),
        NodeId(space="img_space", external_id="col2"),
    ]
    selectors = create_360_image_selectors(collections)

    assert len(selectors) == 3
    collection_selector = selectors[0]
    station_selector = selectors[1]
    image_selector = selectors[2]
    assert isinstance(collection_selector, InstanceViewSelector)
    assert collection_selector.view.external_id == "Image360Collection"
    assert isinstance(station_selector, InstanceQuerySelector)
    assert station_selector.root == "image360"
    assert station_selector.subselections == ("image360station",)
    # 'image360' (root) must be in select, otherwise the query endpoint never emits a cursor for it
    # and pagination silently stops after the first page.
    assert set(station_selector.create_query().select) == {"image360", "image360station"}
    assert isinstance(image_selector, InstanceViewSelector)
    assert image_selector.view.external_id == "Image360"


class TestInFieldLegacyToCDMScheduleMapper:
    SOURCE_SPACE = "source_space"
    TARGET_SPACE = "target_space"
    SCHEDULE_VIEW = ViewId(space="cdf_apm", external_id="Schedule", version="v4")
    DEST_VIEW_ID = ViewId(
        space="cdf_infield",
        external_id="Schedule",
        version="v1",
    )
    CONTAINER_ID = ContainerId(space="cdf_infield", external_id="Schedule")
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

            connection_creator = ConnectionCreator(
                client, instance_id_mapper=SpaceMappingInstanceIdMapper(self.SPACE_MAPPING)
            )
            mapper = InFieldLegacyToCDMScheduleMapper(client, connection_creator, mapping)
            mapper.prepare(MagicMock())

            result = mapper.map([DataItem(tracking_id=str(i), item=s) for i, s in enumerate(schedule_instance_data)])

        mapped_schedules = [data_item.item for data_item in result]
        assert len(mapped_schedules) == 2

        data_regression.check({"schedules": [s.dump() for s in mapped_schedules]})


def _make_record_property_mapping(external_id: str, container_id: ContainerId) -> RecordPropertyMapping:
    return RecordPropertyMapping(
        external_id=external_id,
        container_id=container_id,
        property_mapping={"description": "description"},
    )


def _make_record_container_response(container_id: ContainerId) -> ContainerResponse:
    return ContainerResponse(
        space=container_id.space,
        external_id=container_id.external_id,
        used_for="record",
        properties={
            "description": ContainerPropertyDefinition(
                type=TextProperty(), nullable=True, immutable=False, auto_increment=False
            )
        },
        created_time=0,
        last_updated_time=1,
        is_global=False,
    )


class TestAssetCentricToRecordMapper:
    def test_prepare_raises_on_missing_container(self) -> None:
        container_id = ContainerId(space="my_space", external_id="MissingContainer")
        mapping = _make_record_property_mapping("mapping_x", container_id)
        with monkeypatch_toolkit_client() as client:
            client.tool.containers.retrieve.return_value = []
            mapper = AssetCentricToRecordMapper(client, mappings_by_external_id={"mapping_x": mapping})
            with pytest.raises(ToolkitValueError, match="not found in Data Modeling"):
                mapper.prepare(MagicMock())

    def test_prepare_raises_on_non_record_container(self) -> None:
        container_id = ContainerId(space="my_space", external_id="NodeContainer")
        mapping = _make_record_property_mapping("mapping_x", container_id)
        node_container = _make_record_container_response(container_id)
        node_container.used_for = "node"
        with monkeypatch_toolkit_client() as client:
            client.tool.containers.retrieve.return_value = [node_container]
            mapper = AssetCentricToRecordMapper(client, mappings_by_external_id={"mapping_x": mapping})
            with pytest.raises(ToolkitValueError, match="usedFor='record'"):
                mapper.prepare(MagicMock())

    def test_map_rejects_non_event_row(self) -> None:
        container_id = ContainerId(space="my_space", external_id="EventContainer")
        mapping = _make_record_property_mapping("mapping_a", container_id)
        source = AssetCentricMapping(
            mapping=AssetMapping(
                resource_type="asset",
                instance_id=NodeId(space="my_space", external_id="asset_1"),
                id=1,
                ingestion_mapping="mapping_a",
            ),
            resource=AssetResponse(id=1, name="asset_1", created_time=0, last_updated_time=1, root_id=0),
        )
        with monkeypatch_toolkit_client() as client:
            client.tool.containers.retrieve.return_value = [_make_record_container_response(container_id)]
            mapper = AssetCentricToRecordMapper(client, mappings_by_external_id={"mapping_a": mapping})
            mapper.prepare(MagicMock())
            with pytest.raises(ToolkitValueError, match="only supports Event"):
                mapper.map([DataItem(tracking_id="t", item=source)])

    def test_map_produces_record_request(self) -> None:
        container_id = ContainerId(space="my_space", external_id="EventContainer")
        mapping = _make_record_property_mapping("mapping_a", container_id)
        source = AssetCentricMapping(
            mapping=EventMapping(
                resource_type="event",
                instance_id=NodeId(space="my_space", external_id="event_1"),
                id=42,
                ingestion_mapping="mapping_a",
            ),
            resource=EventResponse(
                id=42, external_id="event_1", description="An event", created_time=0, last_updated_time=1
            ),
        )
        with monkeypatch_toolkit_client() as client:
            client.tool.containers.retrieve.return_value = [_make_record_container_response(container_id)]
            mapper = AssetCentricToRecordMapper(client, mappings_by_external_id={"mapping_a": mapping})
            mapper.prepare(MagicMock())
            results = mapper.map([DataItem(tracking_id="t", item=source)])
        assert len(results) == 1
        record = results[0].item
        assert record is not None
        assert record.space == "my_space"
        assert record.external_id == "event_1"
        assert len(record.sources) == 1
        assert record.sources[0].source == container_id
        assert record.sources[0].properties["description"] == "An event"


class TestImage360AnnotationMapper:
    SOURCE_SPACE = "cdf_360_image_schema"

    @staticmethod
    def _image_node(external_id: str, collection_external_id: str, file_external_id: str) -> NodeResponse:
        return NodeResponse(
            space=TestImage360AnnotationMapper.SOURCE_SPACE,
            external_id=external_id,
            created_time=0,
            last_updated_time=1,
            version=1,
            properties={
                LEGACY_IMAGE360_SOURCE_VIEW: {
                    "collection360": {
                        "space": TestImage360AnnotationMapper.SOURCE_SPACE,
                        "externalId": collection_external_id,
                    },
                    "cubeMapFront": file_external_id,
                }
            },
        )

    def test_prepare_filters_by_selected_collection(self) -> None:
        node_in = self._image_node("image_in", "collection_in", "file_in")
        node_out = self._image_node("image_out", "collection_out", "file_out")
        selector = Image360AnnotationSelector(
            object3d_space="obj_space",
            instance_space="inst_space",
            collections=("collection_in",),
        )

        with monkeypatch_toolkit_client() as client:
            client.tool.instances.list.return_value = [node_in, node_out]
            mapper = Image360AnnotationMapper(client)
            mapper.prepare(selector)

        # Only the face file belonging to the selected collection is loaded.
        assert set(mapper._face_by_file_ext_id) == {"file_in"}

    def test_face_centers_match_fusion_reference(self) -> None:
        """Face centers (u=v=0.5) must match fusion's getNormalizedVectorFromUVAndFace.test.ts."""
        expected = {
            "left": (math.pi / 2, math.pi / 2),
            "right": (math.pi / 2, 3 * math.pi / 2),
            "front": (math.pi, math.pi),
            "back": (0.0, math.pi),
            "top": (math.pi / 2, 0.0),
            "bottom": (math.pi / 2, math.pi),
        }
        for face, (expected_phi, expected_theta) in expected.items():
            phi, theta = Image360AnnotationMapper.uv_and_face_to_spherical(face, 0.5, 0.5)
            assert phi == pytest.approx(expected_phi, abs=1e-4)
            assert theta == pytest.approx(expected_theta, abs=1e-4)

    def test_front_vertex_matches_fusion_transform_annotations(self) -> None:
        """front (0.1, 0.2) must match fusion's transformAnnotationsToVectors.test.ts."""
        phi, theta = Image360AnnotationMapper.uv_and_face_to_spherical("front", 0.1, 0.2)
        assert phi == pytest.approx(2.3562, abs=1e-4)
        assert theta == pytest.approx(0.9273, abs=1e-4)

    def test_map_produces_image360_annotation_item(self) -> None:
        """End-to-end map call: AnnotationResponse → Image360AnnotationItem with spherical polygon."""
        annotation = AnnotationResponse(
            id=42,
            annotation_type="images.AssetLink",
            annotated_resource_type="file",
            annotated_resource_id=111,
            data=ImageAssetLinkData(
                asset_ref=InternalId(id=222),
                text="pump",
                text_region=BoundingBox(x_min=0.0, x_max=0.1, y_min=0.0, y_max=0.1),
                object_region=AnnotationGeometry(
                    polygon=AnnotationPolygon(
                        vertices=[
                            AnnotationPoint(x=0.1, y=0.2),
                            AnnotationPoint(x=0.3, y=0.4),
                            AnnotationPoint(x=0.5, y=0.6),
                        ]
                    )
                ),
            ),
            status="approved",
            creating_app="unit_test",
            creating_app_version="1.0.0",
            creating_user="tester",
            created_time=0,
            last_updated_time=1,
        )
        new_image360_node_id = NodeId(space=self.SOURCE_SPACE, external_id="image_in_cdm")
        asset_node_id = NodeId(space="asset_space", external_id="pump_1")

        with monkeypatch_toolkit_client() as client:
            client.lookup.files.external_id.return_value = "file_in"
            client.migration.lookup.assets.return_value = asset_node_id
            mapper = Image360AnnotationMapper(client)
            mapper._face_by_file_ext_id = {"file_in": ("front", new_image360_node_id)}

            result = mapper.map([DataItem(tracking_id="42", item=annotation)])

        assert len(result) == 1
        item = result[0].item
        assert item.asset.instance_id == asset_node_id
        assert item.image360.instance_id == new_image360_node_id
        # polygon.data: [N, phi1, theta1, ..., phiN, thetaN] for N=3 vertices → 7 floats
        assert len(item.polygon.data) == 7
        assert item.polygon.data[0] == 3.0
        # First vertex (front face, 0.1, 0.2) matches the fusion reference values.
        assert item.polygon.data[1] == pytest.approx(2.3562, abs=1e-4)
        assert item.polygon.data[2] == pytest.approx(0.9273, abs=1e-4)

    def test_map_falls_back_to_text_region_when_no_object_region(self) -> None:
        """When objectRegion polygon is absent, text_region bounding box is used to synthesise a 4-vertex polygon."""
        annotation = AnnotationResponse(
            id=43,
            annotation_type="images.AssetLink",
            annotated_resource_type="file",
            annotated_resource_id=111,
            data=ImageAssetLinkData(
                asset_ref=InternalId(id=222),
                text="pump",
                text_region=BoundingBox(x_min=0.1, x_max=0.5, y_min=0.2, y_max=0.6),
                object_region=None,
            ),
            status="approved",
            creating_app="unit_test",
            creating_app_version="1.0.0",
            creating_user="tester",
            created_time=0,
            last_updated_time=1,
        )
        new_image360_node_id = NodeId(space=self.SOURCE_SPACE, external_id="image_in_cdm")
        asset_node_id = NodeId(space="asset_space", external_id="pump_1")

        with monkeypatch_toolkit_client() as client:
            client.lookup.files.external_id.return_value = "file_in"
            client.migration.lookup.assets.return_value = asset_node_id
            mapper = Image360AnnotationMapper(client)
            mapper._face_by_file_ext_id = {"file_in": ("front", new_image360_node_id)}

            result = mapper.map([DataItem(tracking_id="43", item=annotation)])

        assert len(result) == 1
        item = result[0].item
        assert item.asset.instance_id == asset_node_id
        assert item.image360.instance_id == new_image360_node_id
        # polygon.data: [N, phi1, theta1, ..., phiN, thetaN] for N=4 vertices → 9 floats
        assert item.polygon.data[0] == 4.0
        assert len(item.polygon.data) == 9
