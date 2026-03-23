import json
import uuid
from collections import Counter
from collections.abc import Iterator
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest
import responses
import respx
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling import (
    DataModel,
    DataModelList,
    EdgeApply,
    NodeOrEdgeData,
    View,
)
from cognite.client.data_classes.data_modeling.statistics import InstanceStatistics, ProjectStatistics

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.resource_classes.annotation import AnnotationResponse
from cognite_toolkit._cdf_tk.client.resource_classes.asset import AssetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.canvas import (
    CANVAS_INSTANCE_SPACE,
    CANVAS_VIEW_ID,
    CONTAINER_REFERENCE_VIEW_ID,
    ContainerReferenceItem,
    IndustrialCanvasResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.chart import ChartResponse
from cognite_toolkit._cdf_tk.client.resource_classes.charts_data import ChartData, ChartSource, ChartTimeseries
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import InstanceSource, NodeRequest, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.migration import InstanceSource as LegacyInstanceSource
from cognite_toolkit._cdf_tk.client.resource_classes.streams import (
    LifecycleObject,
    LimitsObject,
    ResourceUsage,
    StreamResponse,
    StreamSettings,
)
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands._migrate.command import MigrationCommand
from cognite_toolkit._cdf_tk.commands._migrate.data_mapper import AssetCentricToInstanceMapper, CanvasMapper, ChartMapper
from cognite_toolkit._cdf_tk.commands._migrate.data_model import (
    COGNITE_MIGRATION_MODEL,
    COGNITE_MIGRATION_SPACE_ID,
    INSTANCE_SOURCE_VIEW_ID,
    MODEL_ID,
    RESOURCE_VIEW_MAPPING_VIEW_ID,
)
from cognite_toolkit._cdf_tk.commands._migrate.default_mappings import (
    ASSET_ANNOTATIONS_ID,
    ASSET_ID,
    FILE_ANNOTATIONS_ID,
    create_default_mappings,
)
from cognite_toolkit._cdf_tk.commands._migrate.migration_io import AnnotationMigrationIO, AssetCentricMigrationIO
from cognite_toolkit._cdf_tk.commands._migrate.selectors import MigrationCSVFileSelector
from cognite_toolkit._cdf_tk.exceptions import ToolkitMigrationError, ToolkitValueError
from cognite_toolkit._cdf_tk.storageio import CanvasIO, ChartIO
from cognite_toolkit._cdf_tk.storageio.progress import CursorBookmark, ProgressYAML
from cognite_toolkit._cdf_tk.storageio.selectors import (
    CanvasExternalIdSelector,
    ChartExternalIdSelector,
)


@pytest.fixture
def cognite_migration_model(
    toolkit_config: ToolkitClientConfig,
    respx_mock: respx.MockRouter,
    cognite_core_no_3D: DataModel[View],
    cognite_extractor_views: list[View],
) -> Iterator[respx.MockRouter]:
    """Mock the Cognite Migration Model in the CDF project."""
    config = toolkit_config
    # Migration model
    migration_model = COGNITE_MIGRATION_MODEL.dump()
    migration_model["createdTime"] = 1
    migration_model["lastUpdatedTime"] = 1
    migration_model["isGlobal"] = False
    respx_mock.post(
        config.create_api_url("models/datamodels/byids"),
    ).respond(
        json={"items": [migration_model]},
    )
    yield respx_mock


@pytest.fixture
def resource_view_mappings(
    toolkit_config: ToolkitClientConfig,
    respx_mock: respx.MockRouter,
    cognite_core_no_3D: DataModel[View],
    cognite_extractor_views: list[View],
) -> Iterator[respx.MockRouter]:
    """Mock all the default Resource View Mappings in the Cognite Migration Model."""
    config = toolkit_config
    mapping_by_id = {mapping.external_id: mapping for mapping in create_default_mappings()}
    node_items: list[dict] = []
    for mapping in mapping_by_id.values():
        # Lookup of the mapping in the Migration Model
        mapping_node_response = mapping.dump(context="api")
        mapping_node_response.update({"createdTime": 0, "lastUpdatedTime": 0, "version": 1})
        sources = mapping_node_response.pop("sources", [])
        if sources:
            mapping_view_id = mapping.VIEW_ID
            mapping_node_response["properties"] = {
                mapping_view_id.space: {
                    f"{mapping_view_id.external_id}/{mapping_view_id.version}": sources[0]["properties"]
                }
            }
        node_items.append(mapping_node_response)
    respx_mock.post(
        config.create_api_url("models/instances/byids"),
    ).respond(json={"items": node_items})
    respx_mock.post(
        config.create_api_url("models/views/byids"),
    ).respond(
        json={
            "items": [view.dump() for view in cognite_core_no_3D.views]
            + [view.dump() for view in cognite_extractor_views]
        },
    )
    yield respx_mock


@pytest.fixture
def mock_statistics(
    toolkit_config: ToolkitClientConfig,
    rsps: responses.RequestsMock,
) -> Iterator[responses.RequestsMock]:
    config = toolkit_config
    stats_response = {
        "spaces": {
            "count": 0,
            "limit": 1_000,
        },
        "containers": {
            "count": 0,
            "limit": 10_000,
        },
        "views": {
            "count": 0,
            "limit": 100_000,
        },
        "dataModels": {
            "count": 1,
            "limit": 10_000,
        },
        "containerProperties": {
            "count": 0,
            "limit": 1_000_000,
        },
        "instances": {
            "nodes": 1000,
            "edges": 0,
            "softDeletedNodes": 0,
            "softDeletedEdges": 0,
            "instancesLimit": 5_000_000,
            "softDeletedInstancesLimit": 100_000_000,
            "instances": 1000,
            "softDeletedInstances": 0,
        },
        "concurrentReadLimit": 50,
        "concurrentWriteLimit": 20,
        "concurrentDeleteLimit": 10,
    }
    rsps.get(
        config.create_api_url("/models/statistics"),
        json=stats_response,
        status=200,
    )
    yield rsps


@pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
class TestMigrationCommand:
    @pytest.mark.usefixtures("mock_statistics", "resource_view_mappings")
    def test_migrate_assets(
        self,
        toolkit_config: ToolkitClientConfig,
        tmp_path: Path,
        cognite_migration_model: respx.MockRouter,
    ) -> None:
        respx_mock = cognite_migration_model
        config = toolkit_config
        assets = [
            AssetResponse(
                id=1000 + i,
                external_id=f"asset_{i}",
                name=f"Asset {i}",
                description=f"This is Asset {i}",
                last_updated_time=1,
                created_time=0,
                parent_external_id="asset_0" if i > 0 else None,
                root_id=1,
            )
            for i in range(2)
        ]
        space = "my_space"
        csv_content = (
            "id,space,externalId,ingestionView,consumerViewSpace,consumerViewExternalId,consumerViewVersion\n"
            + "\n".join(f"{1000 + i},{space},asset_{i},{ASSET_ID},cdf_cdm,CogniteAsset,v1" for i in range(len(assets)))
        )

        # Asset retrieve ids
        respx.post(
            config.create_api_url("/assets/byids"),
        ).mock(
            return_value=httpx.Response(
                status_code=200,
                json={"items": [asset.dump() for asset in assets]},
            )
        )

        # Instance creation
        respx.post(
            config.create_api_url("/models/instances"),
        ).mock(
            return_value=httpx.Response(
                status_code=200,
                json={
                    "items": [
                        {
                            "instanceType": "node",
                            "space": space,
                            "externalId": f"asset_{i}",
                            "version": 1,
                            "wasModified": True,
                            "createdTime": 1,
                            "lastUpdatedTime": 1,
                        }
                        for i in range(len(assets))
                    ]
                },
            )
        )
        csv_file = tmp_path / "migration.csv"
        csv_file.write_text(csv_content, encoding="utf-8")

        client = ToolkitClient(config)
        command = MigrationCommand(silent=True)
        selector = MigrationCSVFileSelector(datafile=csv_file, kind="Assets")
        logs = tmp_path / "logs"
        results_by_selector = command.migrate(
            selectors=[selector],
            data=AssetCentricMigrationIO(client),
            mapper=AssetCentricToInstanceMapper(client),
            log_dir=logs,
            dry_run=False,
            verbose=False,
        )

        # Check that the assets were uploaded
        last_call = respx_mock.calls[-1]
        assert last_call.request.url == config.create_api_url("/models/instances")
        assert last_call.request.method == "POST"
        actual_instances = json.loads(last_call.request.content)["items"]
        expected_instance = [
            NodeRequest(
                space=space,
                external_id=asset.external_id,
                sources=[
                    InstanceSource(
                        source=ViewId(space="cdf_cdm", external_id="CogniteAsset", version="v1"),
                        properties={
                            "name": asset.name,
                            "description": asset.description,
                        },
                    ),
                    InstanceSource(
                        source=INSTANCE_SOURCE_VIEW_ID,
                        properties={
                            "id": asset.id,
                            "resourceType": "asset",
                            "dataSetId": None,
                            "classicExternalId": asset.external_id,
                            "resourceViewMapping": {"space": COGNITE_MIGRATION_SPACE_ID, "externalId": ASSET_ID},
                            "preferredConsumerViewId": {
                                "space": "cdf_cdm",
                                "externalId": "CogniteAsset",
                                "version": "v1",
                                "type": "view",
                            },
                        },
                    ),
                ],
            ).dump()
            for asset in assets
        ]
        assert actual_instances == expected_instance
        result = results_by_selector[str(selector)]
        actual_results = {status.status: status.count for status in result}
        assert actual_results == {"failure": 0, "pending": 0, "success": len(assets), "unchanged": 0, "skipped": 0}

    @pytest.mark.usefixtures("mock_statistics", "resource_view_mappings")
    def test_migrate_resume(
        self,
        toolkit_config: ToolkitClientConfig,
        tmp_path: Path,
        cognite_migration_model: respx.MockRouter,
    ) -> None:
        respx_mock = cognite_migration_model
        config = toolkit_config
        assets = [
            AssetResponse(
                id=1000,
                external_id="asset_0",
                name="Asset 0",
                description="This is Asset 0",
                last_updated_time=1,
                created_time=0,
                root_id=1,
            )
        ]
        space = "my_space"
        csv_content = (
            "id,space,externalId,ingestionView,consumerViewSpace,consumerViewExternalId,consumerViewVersion\n"
            f"{assets[0].id},{space},{assets[0].external_id},{ASSET_ID},cdf_cdm,CogniteAsset,v1"
        )

        # Asset retrieve ids
        respx.post(
            config.create_api_url("/assets/byids"),
        ).mock(
            return_value=httpx.Response(
                status_code=200,
                json={"items": [asset.dump() for asset in assets]},
            )
        )

        # Instance creation
        respx_mock.post(
            config.create_api_url("/models/instances"),
        ).mock(
            return_value=httpx.Response(
                status_code=200,
                json={
                    "items": [
                        {
                            "instanceType": "node",
                            "space": space,
                            "externalId": assets[0].external_id,
                            "version": 1,
                            "wasModified": True,
                            "createdTime": 1,
                            "lastUpdatedTime": 1,
                        }
                    ]
                },
            )
        )
        csv_file = tmp_path / "migration.csv"
        csv_file.write_text(csv_content, encoding="utf-8")
        selector = MigrationCSVFileSelector(datafile=csv_file, kind="Assets")
        logs = tmp_path / "logs"
        ProgressYAML(
            status="in-progress",
            bookmarks={"main": CursorBookmark(cursor="resume-cursor-1")},
            total=1,
            completed_count=0,
        ).dump_to_file(logs, filestem=str(selector))

        observed_bookmarks = []
        original_stream_data = AssetCentricMigrationIO.stream_data

        def _track_resume_bookmark(self, selector, limit=None, bookmark=None):
            observed_bookmarks.append(bookmark)
            yield from original_stream_data(self, selector, limit=limit, bookmark=bookmark)

        client = ToolkitClient(config)
        command = MigrationCommand(silent=True)
        with patch.object(AssetCentricMigrationIO, "stream_data", autospec=True, side_effect=_track_resume_bookmark):
            results_by_selector = command.migrate(
                selectors=[selector],
                data=AssetCentricMigrationIO(client),
                mapper=AssetCentricToInstanceMapper(client),
                log_dir=logs,
                dry_run=False,
                verbose=False,
            )

        assert len(observed_bookmarks) == 1
        bookmark = observed_bookmarks[0]
        assert isinstance(bookmark, CursorBookmark)
        assert bookmark.cursor == "resume-cursor-1"

        result = results_by_selector[str(selector)]
        actual_results = {status.status: status.count for status in result}
        assert actual_results == {"failure": 0, "pending": 0, "success": 1, "unchanged": 0, "skipped": 0}

        progress = ProgressYAML.try_load(logs, filestem=str(selector))
        assert progress is not None
        assert progress.status == "completed"

    @pytest.mark.usefixtures("mock_statistics", "resource_view_mappings")
    def test_migrate_annotations(
        self,
        toolkit_config: ToolkitClientConfig,
        cognite_migration_model: respx.MockRouter,
        tmp_path: Path,
    ) -> None:
        respx_mock = cognite_migration_model
        config = toolkit_config
        asset_annotation = AnnotationResponse(
            id=2000,
            annotated_resource_type="file",
            annotated_resource_id=3000,
            data={
                "assetRef": {"id": 4000},
                "textRegion": {"xMin": 10.0, "xMax": 100.0, "yMin": 20.0, "yMax": 200.0},
            },
            status="approved",
            creating_user="doctrino",
            creating_app="my_app",
            creating_app_version="v1",
            annotation_type="diagrams.AssetLink",
            created_time=0,
            last_updated_time=0,
        )
        file_annotation = AnnotationResponse(
            id=2001,
            annotated_resource_type="file",
            annotated_resource_id=3001,
            data={
                "fileRef": {"id": 5000},
                "textRegion": {"xMin": 15.0, "xMax": 150.0, "yMin": 25.0, "yMax": 250.0},
            },
            status="approved",
            creating_user="doctrino",
            creating_app="my_app",
            creating_app_version="v1",
            annotation_type="diagrams.FileLink",
            created_time=0,
            last_updated_time=0,
        )
        annotations = [asset_annotation, file_annotation]
        space = "my_space"
        csv_content = "id,space,externalId,ingestionView\n" + "\n".join(
            (
                f"{2000},{space},annotation_{2000},{ASSET_ANNOTATIONS_ID}",
                f"{2001},{space},annotation_{2001},{FILE_ANNOTATIONS_ID}",
            )
        )
        # Annotation retrieve ids (toolkit API uses httpx)
        respx_mock.post(config.create_api_url("/annotations/byids")).mock(
            return_value=httpx.Response(
                status_code=200,
                json={"items": [annotation.dump() for annotation in annotations]},
            )
        )
        # Lookup asset and file instance ID
        query_responses = []
        for items in [
            [("asset", 4000)],
            [("file", 5000), ("file", 3000), ("file", 3001)],
        ]:
            query_responses.append(
                httpx.Response(
                    status_code=200,
                    json={
                        "items": {
                            "instanceSource": [
                                {
                                    "instanceType": "node",
                                    "space": space,
                                    "externalId": f"{resource_type}_{resource_id}",
                                    "version": 0,
                                    "createdTime": 0,
                                    "lastUpdatedTime": 0,
                                    "properties": {
                                        "cognite_migration": {
                                            "InstanceSource/v1": {
                                                "id": resource_id,
                                                "resourceType": resource_type,
                                            }
                                        },
                                    },
                                }
                                for (resource_type, resource_id) in items
                            ],
                        },
                        "nextCursor": {"instanceSource": None},
                    },
                )
            )
        respx_mock.post(config.create_api_url("/models/instances/query")).mock(
            side_effect=query_responses,
        )

        # Instance creation
        respx.post(
            config.create_api_url("/models/instances"),
        ).mock(
            return_value=httpx.Response(
                status_code=200,
                json={
                    "items": [
                        {
                            "instanceType": "edge",
                            "space": space,
                            "externalId": f"annotation_{2000 + i}",
                            "version": 1,
                            "wasModified": True,
                            "createdTime": 1,
                            "lastUpdatedTime": 1,
                        }
                        for i in range(len(annotations))
                    ]
                },
            )
        )
        csv_file = tmp_path / "migration.csv"
        csv_file.write_text(csv_content, encoding="utf-8")

        client = ToolkitClient(config)
        command = MigrationCommand(silent=True)

        selector = MigrationCSVFileSelector(datafile=csv_file, kind="Annotations")
        results_by_selector = command.migrate(
            selectors=[selector],
            data=AnnotationMigrationIO(client),
            mapper=AssetCentricToInstanceMapper(client),
            log_dir=tmp_path / "logs",
            dry_run=False,
            verbose=True,
        )
        result = results_by_selector[str(selector)]
        actual_results = {status.status: status.count for status in result}
        assert actual_results == {"failure": 0, "pending": 0, "success": len(annotations), "unchanged": 0, "skipped": 0}

        # Check that the annotations were uploaded
        last_call = respx_mock.calls[-1]
        assert last_call.request.url == config.create_api_url("/models/instances")
        assert last_call.request.method == "POST"
        actual_instances = json.loads(last_call.request.content)["items"]
        expected_instance = [
            EdgeApply(
                space=space,
                external_id=f"annotation_{asset_annotation.id}",
                start_node=(space, f"file_{asset_annotation.annotated_resource_id}"),
                end_node=(space, f"asset_{asset_annotation.data['assetRef']['id']}"),
                type=(space, asset_annotation.annotation_type),
                sources=[
                    NodeOrEdgeData(
                        source=dm.ViewId("cdf_cdm", "CogniteDiagramAnnotation", "v1"),
                        properties={
                            "sourceContext": asset_annotation.creating_app_version,
                            "sourceCreatedUser": asset_annotation.creating_user,
                            "sourceId": asset_annotation.creating_app,
                            "status": "Approved",
                            "startNodeXMax": asset_annotation.data["textRegion"]["xMax"],
                            "startNodeXMin": asset_annotation.data["textRegion"]["xMin"],
                            "startNodeYMax": asset_annotation.data["textRegion"]["yMax"],
                            "startNodeYMin": asset_annotation.data["textRegion"]["yMin"],
                        },
                    ),
                ],
            ).dump(),
            EdgeApply(
                space=space,
                external_id=f"annotation_{file_annotation.id}",
                start_node=(space, f"file_{file_annotation.annotated_resource_id}"),
                end_node=(space, f"file_{file_annotation.data['fileRef']['id']}"),
                type=(space, file_annotation.annotation_type),
                sources=[
                    NodeOrEdgeData(
                        source=dm.ViewId("cdf_cdm", "CogniteDiagramAnnotation", "v1"),
                        properties={
                            "sourceContext": file_annotation.creating_app_version,
                            "sourceCreatedUser": file_annotation.creating_user,
                            "sourceId": file_annotation.creating_app,
                            "status": "Approved",
                            "startNodeXMax": file_annotation.data["textRegion"]["xMax"],
                            "startNodeXMin": file_annotation.data["textRegion"]["xMin"],
                            "startNodeYMax": file_annotation.data["textRegion"]["yMax"],
                            "startNodeYMin": file_annotation.data["textRegion"]["yMin"],
                        },
                    ),
                ],
            ).dump(),
        ]
        assert actual_instances == expected_instance

    def test_migrate_charts(
        self,
        toolkit_config: ToolkitClientConfig,
        cognite_migration_model: respx.MockRouter,
        tmp_path: Path,
    ) -> None:
        respx_mock = cognite_migration_model
        config = toolkit_config
        charts = [
            ChartResponse(
                external_id="my_chart",
                created_time=1,
                last_updated_time=1,
                visibility="PUBLIC",
                data=ChartData(
                    version=1,
                    name="My Chart",
                    date_from="2025-01-01T00:00:00.000Z",
                    date_to="2025-12-31T23:59:59.999Z",
                    time_series_collection=[
                        ChartTimeseries(
                            tsExternalId="ts_1", type="timeseries", id="87654321-4321-8765-4321-876543218765"
                        ),
                        ChartTimeseries(tsId=1, type="timeseries", id="12345678-1234-5678-1234-567812345678"),
                    ],
                    source_collection=[
                        ChartSource(type="timeseries", id="87654321-4321-8765-4321-876543218765"),
                        ChartSource(type="timeseries", id="12345678-1234-5678-1234-567812345678"),
                    ],
                ),
                owner_id="1234",
            )
        ]
        # Inspect response
        respx.get(
            f"{config.base_url}/api/v1/token/inspect",
        ).respond(
            json={
                "subject": "test",
                "projects": [{"projectUrlName": "pytest-project", "groups": []}],
                "capabilities": [
                    {
                        "chartsAdminAcl": {"actions": ["READ", "UPDATE"], "scope": {"all": {}}},
                        "projectScope": {"allProjects": {}},
                    }
                ],
            }
        )
        # Chart list
        respx.post(
            config.create_app_url("/storage/charts/charts/list"),
        ).respond(
            status_code=200,
            json={
                "items": [chart.dump() for chart in charts],
            },
        )
        # TimeSeries Instance ID lookup (uses toolkit InstancesAPI → httpx)
        respx_mock.post(config.create_api_url("/models/instances/query")).mock(
            return_value=httpx.Response(
                status_code=200,
                json={
                    "items": {
                        "instanceSource": [
                            LegacyInstanceSource(
                                space="my_space",
                                external_id="node_123",
                                version=1,
                                last_updated_time=1,
                                created_time=1,
                                resource_type="timeseries",
                                id_=1,
                                classic_external_id=None,
                                preferred_consumer_view_id=ViewId(
                                    space="cdf_cdm", external_id="CogniteTimeSeries", version="v1"
                                ),
                            ).dump(),
                            LegacyInstanceSource(
                                space="my_space",
                                external_id="node_ts_1",
                                version=1,
                                last_updated_time=1,
                                created_time=1,
                                resource_type="timeseries",
                                id_=2,
                                classic_external_id="ts_1",
                                preferred_consumer_view_id=ViewId(
                                    space="my_schema_space", external_id="MyTimeSeries", version="v1"
                                ),
                            ).dump(),
                        ]
                    },
                    "nextCursor": {"instanceSource": None},
                },
            )
        )

        # Chart update (existing chart goes through per-chart update endpoint)
        respx.put(
            config.create_app_url("/storage/charts/charts/my_chart"),
        ).mock(return_value=httpx.Response(status_code=200, json={"items": [charts[0].dump()]}))

        client = ToolkitClient(config)
        command = MigrationCommand(silent=True)
        new_uuids = [
            uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
            uuid.UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
        ]
        selector = ChartExternalIdSelector(external_ids=("my_chart",))
        with patch(f"{ChartMapper.__module__}.uuid4", side_effect=new_uuids):
            results_by_selector = command.migrate(
                selectors=[selector],
                data=ChartIO(client),
                mapper=ChartMapper(client),
                log_dir=tmp_path / "logs",
                dry_run=False,
                verbose=True,
            )
        result = results_by_selector[str(selector)]
        actual_results = {status.status: status.count for status in result}
        assert actual_results == {"failure": 0, "pending": 0, "success": len(charts), "unchanged": 0, "skipped": 0}

        calls = respx_mock.calls
        assert len(calls) == 5
        last_call = calls[-1]
        assert last_call.request.url == config.create_app_url("/storage/charts/charts/my_chart")
        assert last_call.request.method == "PUT"
        actual_chart = json.loads(last_call.request.content)
        expected_chart = {
            "visibility": "PUBLIC",
            "data": {
                "version": 1,
                "name": "My Chart",
                "dateFrom": "2025-01-01T00:00:00.000Z",
                "dateTo": "2025-12-31T23:59:59.999Z",
                "coreTimeseriesCollection": [
                    {
                        "type": "coreTimeseries",
                        "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                        "nodeReference": {"space": "my_space", "externalId": "node_ts_1"},
                        "viewReference": {
                            "space": "my_schema_space",
                            "externalId": "MyTimeSeries",
                            "version": "v1",
                        },
                    },
                    {
                        "type": "coreTimeseries",
                        "id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                        "nodeReference": {"space": "my_space", "externalId": "node_123"},
                        "viewReference": {"space": "cdf_cdm", "externalId": "CogniteTimeSeries", "version": "v1"},
                    },
                ],
                "sourceCollection": [
                    {
                        "type": "coreTimeseries",
                        "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                    },
                    {
                        "type": "coreTimeseries",
                        "id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                    },
                ],
                "timeSeriesCollection": None,
            },
        }
        assert actual_chart == expected_chart

    @pytest.mark.usefixtures("mock_statistics")
    def test_migrate_canvas(
        self,
        toolkit_config: ToolkitClientConfig,
        cognite_migration_model: respx.MockRouter,
        tmp_path: Path,
        asset_centric_canvas: tuple[IndustrialCanvasResponse, list[LegacyInstanceSource]],
    ) -> None:
        respx_mock = cognite_migration_model
        config = toolkit_config
        canvas, instance_sources = asset_centric_canvas
        id_ = uuid.uuid4()
        non_existing_file = ContainerReferenceItem(
            external_id=f"{canvas.external_id}_{id_!s}",
            container_reference_type="file",
            resource_id=999,
            id_=str(id_),
        )

        def _to_query_node(space, external_id, view_id, props, version=1, created_time=0, last_updated_time=0):
            """Build a DMS query response node with properties nested under space/view."""
            return {
                "instanceType": "node",
                "space": space,
                "externalId": external_id,
                "version": version,
                "createdTime": created_time,
                "lastUpdatedTime": last_updated_time,
                "properties": {
                    view_id.space: {
                        f"{view_id.external_id}/{view_id.version}": props,
                    }
                },
            }

        def _item_to_query_node(item, view_id):
            props = item.model_dump(mode="json", by_alias=True, exclude_unset=True, exclude={"space", "external_id"})
            return _to_query_node(item.space, item.external_id, view_id, props)

        canvas_props = canvas.model_dump(
            mode="json",
            by_alias=True,
            exclude_unset=True,
            exclude={
                "instance_type",
                "space",
                "external_id",
                "version",
                "created_time",
                "last_updated_time",
                "annotations",
                "container_references",
                "fdm_instance_container_references",
                "solution_tag_items",
            },
        )
        canvas_node = _to_query_node(
            canvas.space,
            canvas.external_id,
            CANVAS_VIEW_ID,
            canvas_props,
            version=canvas.version,
            created_time=canvas.created_time,
            last_updated_time=canvas.last_updated_time,
        )

        container_ref_nodes = [
            _item_to_query_node(ref, CONTAINER_REFERENCE_VIEW_ID) for ref in (canvas.container_references or [])
        ] + [_item_to_query_node(non_existing_file, CONTAINER_REFERENCE_VIEW_ID)]

        canvas_query_data = {
            "items": {
                "canvas": [canvas_node],
                "solutionTags": [],
                "annotations": [],
                "containerReferences": container_ref_nodes,
                "fdmInstanceContainerReferences": [],
            },
            "nextCursor": {
                "canvas": None,
                "solutionTags": None,
                "annotations": None,
                "containerReferences": None,
                "fdmInstanceContainerReferences": None,
            },
        }
        empty_query_data = {"items": {"canvas": []}, "nextCursor": {}}
        canvas_query_done = False

        def _query_side_effect(request: httpx.Request) -> httpx.Response:
            nonlocal canvas_query_done
            body = json.loads(request.content)
            with_keys = set(body.get("with", {}).keys())
            if "instanceSource" in with_keys:
                return httpx.Response(
                    status_code=200,
                    json={
                        "items": {
                            "instanceSource": [
                                instance_source.dump()
                                for instance_source in instance_sources
                                if instance_source.resource_type
                                in {
                                    v
                                    for f in body.get("with", {})
                                    .get("instanceSource", {})
                                    .get("nodes", {})
                                    .get("filter", {})
                                    .get("and", [])
                                    if (v := f.get("equals", {}).get("value")) is not None
                                }
                            ]
                        },
                        "nextCursor": {"instanceSource": None},
                    },
                )
            if not canvas_query_done:
                canvas_query_done = True
                return httpx.Response(status_code=200, json=canvas_query_data)
            return httpx.Response(status_code=200, json=empty_query_data)

        respx_mock.post(config.create_api_url("/models/instances/query")).mock(
            side_effect=_query_side_effect,
        )

        def _echo_upsert_items(request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content)
            items = [
                {
                    "instanceType": item.get("instanceType", "node"),
                    "space": item.get("space", CANVAS_INSTANCE_SPACE),
                    "externalId": item.get("externalId", "unknown"),
                    "version": 1,
                    "wasModified": True,
                    "createdTime": 0,
                    "lastUpdatedTime": 0,
                }
                for item in body.get("items", [])
            ]
            return httpx.Response(status_code=200, json={"items": items})

        respx_mock.post(config.create_api_url("/models/instances")).mock(
            side_effect=_echo_upsert_items,
        )

        client = ToolkitClient(config)
        command = MigrationCommand(silent=True)

        selector = CanvasExternalIdSelector(external_ids=(canvas.external_id,))
        results_by_selector = command.migrate(
            selectors=[selector],
            data=CanvasIO(client, exclude_existing_version=True),
            mapper=CanvasMapper(client, dry_run=False, skip_on_missing_ref=False),
            log_dir=tmp_path / "logs",
            dry_run=False,
            verbose=False,
        )

        result = results_by_selector[str(selector)]
        actual_results = {status.status: status.count for status in result}
        assert actual_results == {"failure": 0, "pending": 0, "success": 1, "unchanged": 0, "skipped": 0}

        upsert_calls = [
            c
            for c in respx_mock.calls
            if str(c.request.url) == config.create_api_url("/models/instances") and c.request.method == "POST"
        ]
        assert len(upsert_calls) >= 1
        call = upsert_calls[-1]
        created_instance = json.loads(call.request.content.decode("utf-8"))["items"]
        created_nodes = Counter(
            item["sources"][0]["source"]["externalId"] for item in created_instance if item["instanceType"] == "node"
        )
        asset_centric_ref_count = sum(
            1
            for ref in (canvas.container_references or [])
            if ref.container_reference_type in CanvasMapper.asset_centric_resource_types
        )
        assert created_nodes == {
            "Canvas": 1,
            "ContainerReference": len(canvas.container_references or []) - asset_centric_ref_count,
            "FdmInstanceContainerReference": len(canvas.fdm_instance_container_references or [])
            + asset_centric_ref_count,
        }

        has_existing_version = [item["externalId"] for item in created_instance if "existingVersion" in item]
        assert not has_existing_version, f"Expected no existingVersion field, but found in: {has_existing_version}"

    def test_validate_migration_model_available(self) -> None:
        with monkeypatch_toolkit_client() as client:
            client.data_modeling.data_models.retrieve.return_value = DataModelList([])
            with pytest.raises(ToolkitMigrationError):
                MigrationCommand.validate_migration_model_available(client)

    def test_validate_migration_model_available_multiple_models(self) -> None:
        """Test that multiple models raises an error."""
        with monkeypatch_toolkit_client() as client:
            # Create mock models with the expected MODEL_ID
            model1 = MagicMock(spec=DataModel)
            model1.as_id.return_value = MODEL_ID
            model2 = MagicMock(spec=DataModel)
            model2.as_id.return_value = MODEL_ID

            client.tool.data_models.retrieve.return_value = DataModelList([model1, model2])

            with pytest.raises(ToolkitMigrationError) as exc_info:
                MigrationCommand.validate_migration_model_available(client)

            assert "Multiple migration models" in str(exc_info.value)

    def test_validate_migration_model_available_missing_views(self) -> None:
        """Test that a model with missing views raises an error."""
        with monkeypatch_toolkit_client() as client:
            model = MagicMock(spec=DataModel)
            model.as_id.return_value = MODEL_ID
            # Model has views but missing the required ones
            model.views = [INSTANCE_SOURCE_VIEW_ID]  # Missing VIEW_SOURCE_VIEW_ID

            client.data_modeling.data_models.retrieve.return_value = DataModelList([model])

            with pytest.raises(ToolkitMigrationError, match=r"Invalid migration model. Missing views"):
                MigrationCommand.validate_migration_model_available(client)

    def test_validate_migration_model_available_success(self) -> None:
        """Test that a valid model with all required views succeeds."""
        with monkeypatch_toolkit_client() as client:
            # Mocking the migration Model to get a response format of the model.
            # An alternative would be to write a conversion of write -> read format of the model
            # which is a significant amount of logic.
            model = MagicMock(spec=DataModel)
            model.as_id.return_value = MODEL_ID
            # Model has all required views
            model.views = [INSTANCE_SOURCE_VIEW_ID, RESOURCE_VIEW_MAPPING_VIEW_ID]

            client.tool.data_models.retrieve.return_value = DataModelList([model])

            # Should not raise any exception
            MigrationCommand.validate_migration_model_available(client)

            client.tool.data_models.retrieve.assert_called_once_with([MODEL_ID], inline_views=False)

    def test_validate_available_capacity_missing_capacity(self) -> None:
        cmd = MigrationCommand(silent=True)

        with monkeypatch_toolkit_client() as client:
            stats = MagicMock(spec=ProjectStatistics)
            stats.instances = InstanceStatistics(
                nodes=1000,
                edges=0,
                soft_deleted_edges=0,
                soft_deleted_nodes=0,
                instances_limit=1500,
                soft_deleted_instances_limit=10_000,
                instances=1000,
                soft_deleted_instances=0,
            )
            client.data_modeling.statistics.project.return_value = stats
            with pytest.raises(ToolkitValueError) as exc_info:
                cmd.validate_available_capacity(client, 10_000)

        assert "Cannot proceed with migration" in str(exc_info.value)

    def test_validate_available_capacity_sufficient_capacity(self) -> None:
        cmd = MigrationCommand(silent=True)

        with monkeypatch_toolkit_client() as client:
            stats = MagicMock(spec=ProjectStatistics)
            stats.instances = InstanceStatistics(
                nodes=1000,
                edges=0,
                soft_deleted_edges=0,
                soft_deleted_nodes=0,
                instances_limit=5_000_000,
                soft_deleted_instances_limit=100_000_000,
                instances=1000,
                soft_deleted_instances=0,
            )
            client.data_modeling.statistics.project.return_value = stats
            cmd.validate_available_capacity(client, 10_000)

    @pytest.mark.parametrize(
        "existing_files, stem, expected",
        [
            ([], "migration_log", "migration_log-"),
            (["migration_log-part001.log"], "migration_log", "migration_log-run2-"),
            (["migration_log-part001.log", "migration_log-run3-part001.log"], "migration_log", "migration_log-run4-"),
        ],
    )
    def test_create_logfile_stem(self, existing_files: list[str], stem: str, expected: str, tmp_path: Path) -> None:
        for filename in existing_files:
            (tmp_path / filename).touch()
        actual = MigrationCommand._create_logfile_stem(tmp_path, stem, "not_important")

        assert actual == expected

    def _make_stream(
        self, provisioned_records: int, consumed_records: int, provisioned_gb: float = 100.0, consumed_gb: float = 0.0
    ) -> StreamResponse:
        return StreamResponse(
            external_id="my_stream",
            created_time=0,
            created_from_template="t",
            type="Mutable",
            settings=StreamSettings(
                lifecycle=LifecycleObject(retained_after_soft_delete="P30D"),
                limits=LimitsObject(
                    max_records_total=ResourceUsage(provisioned=provisioned_records, consumed=consumed_records),
                    max_giga_bytes_total=ResourceUsage(provisioned=provisioned_gb, consumed=consumed_gb),
                ),
            ),
        )

    def test_validate_stream_capacity_sufficient(self) -> None:
        stream = self._make_stream(provisioned_records=1_000_000, consumed_records=100_000)
        cmd = MigrationCommand(silent=True)
        with monkeypatch_toolkit_client() as client:
            cmd.validate_stream_capacity(client, stream, 500_000)  # should not raise

    def test_validate_stream_capacity_insufficient_records(self) -> None:
        stream = self._make_stream(provisioned_records=1_000, consumed_records=900)
        cmd = MigrationCommand(silent=True)
        with monkeypatch_toolkit_client() as client:
            with pytest.raises(ToolkitValueError, match="enough record capacity"):
                cmd.validate_stream_capacity(client, stream, 200)

    def test_validate_stream_capacity_insufficient_storage(self) -> None:
        stream = self._make_stream(
            provisioned_records=1_000_000, consumed_records=0, provisioned_gb=10.0, consumed_gb=10.0
        )
        cmd = MigrationCommand(silent=True)
        with monkeypatch_toolkit_client() as client:
            with pytest.raises(ToolkitValueError, match="enough storage capacity"):
                cmd.validate_stream_capacity(client, stream, 1)
