import itertools
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest
import respx
from cognite.client import data_modeling as dm
from cognite.client.data_classes.capabilities import (
    DataModelInstancesAcl,
    DataModelsAcl,
    FilesAcl,
    TimeSeriesAcl,
)
from cognite.client.data_classes.data_modeling import NodeList, Space
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteFile, CogniteTimeSeries
from cognite.client.data_classes.data_modeling.statistics import InstanceStatistics, SpaceStatistics

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.identifiers import NodeId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    ContainerResponse,
    DataModelResponse,
    EdgeResponse,
    NodeResponse,
    SpaceResponse,
    ViewResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataResponse
from cognite_toolkit._cdf_tk.client.resource_classes.timeseries import TimeSeriesResponse
from cognite_toolkit._cdf_tk.commands import PurgeCommand
from cognite_toolkit._cdf_tk.commands._purge import validate_soft_delete_purge_headroom
from cognite_toolkit._cdf_tk.dataio.selectors import InstanceViewSelector, SelectedView
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from tests.test_unit.utils import FakeCogniteResourceGenerator


@pytest.fixture(scope="session")
def cognite_timeseries_2000_list() -> NodeList[CogniteTimeSeries]:
    return NodeList[CogniteTimeSeries](
        [
            CogniteTimeSeries(
                space="test_space",
                external_id=f"test_timeseries_{i}",
                version=1,
                last_updated_time=1,
                created_time=1,
                is_step=True,
                time_series_type="numeric",
            )
            for i in range(2000)
        ]
    )


@pytest.fixture(scope="session")
def cognite_files_2000_list() -> NodeList[CogniteFile]:
    return NodeList[CogniteFile](
        [
            CogniteFile(
                space="test_space",
                external_id=f"test_file_{i}",
                version=1,
                last_updated_time=1,
                created_time=1,
                name=f"test_file_{i}.txt",
                mime_type="text/plain",
            )
            for i in range(2000)
        ]
    )


@pytest.fixture()
def timeseries_by_node_id(
    cognite_timeseries_2000_list: NodeList[CogniteTimeSeries],
) -> dict[dm.NodeId, dict[str, Any]]:
    result: dict[dm.NodeId, dict[str, Any]] = {}
    for i, ts in enumerate(cognite_timeseries_2000_list):
        node_id = ts.as_id()
        ref = NodeId(space=node_id.space, external_id=node_id.external_id)
        result[node_id] = TimeSeriesResponse(
            id=i,
            external_id=ts.external_id,
            instance_id=ref,
            is_string=ts.time_series_type == "string",
            is_step=ts.is_step,
            pending_instance_id=ref,
            type="numeric",
            created_time=1,
            last_updated_time=1,
        ).dump(camel_case=True)
    return result


@pytest.fixture()
def files_by_node_id(
    cognite_files_2000_list: NodeList[CogniteFile],
) -> dict[dm.NodeId, dict[str, Any]]:
    result: dict[dm.NodeId, dict[str, Any]] = {}
    for i, file in enumerate(cognite_files_2000_list):
        node_id = file.as_id()
        ref = NodeId(space=node_id.space, external_id=node_id.external_id)
        result[node_id] = FileMetadataResponse(
            id=i,
            external_id=file.external_id,
            instance_id=ref,
            name=file.name,
            mime_type=file.mime_type,
            pending_instance_id=ref,
            created_time=1,
            last_updated_time=1,
            uploaded=file.is_uploaded or True,
        ).dump(camel_case=True)
    return result


@pytest.fixture()
def purge_responses(
    respx_mock: respx.MockRouter,
    toolkit_config: ToolkitClientConfig,
) -> Iterator[respx.MockRouter]:
    config = toolkit_config
    respx_mock.get(f"{config.base_url}/api/v1/token/inspect").respond(
        status_code=200,
        json={
            "subject": "123",
            "projects": [],
            "capabilities": [
                {
                    "projectScope": {"allProjects": {}},
                    **TimeSeriesAcl(
                        actions=[TimeSeriesAcl.Action.Read, TimeSeriesAcl.Action.Write],
                        scope=TimeSeriesAcl.Scope.All(),
                    ).dump(),
                },
                {
                    "projectScope": {"allProjects": {}},
                    **DataModelsAcl(
                        actions=[DataModelsAcl.Action.Read, DataModelsAcl.Action.Write], scope=DataModelsAcl.Scope.All()
                    ).dump(),
                },
                {
                    "projectScope": {"allProjects": {}},
                    **DataModelInstancesAcl(
                        actions=[DataModelInstancesAcl.Action.Read, DataModelInstancesAcl.Action.Write],
                        scope=DataModelInstancesAcl.Scope.All(),
                    ).dump(),
                },
                {
                    "projectScope": {"allProjects": {}},
                    **FilesAcl(
                        actions=[FilesAcl.Action.Read, FilesAcl.Action.Write], scope=FilesAcl.Scope.All()
                    ).dump(),
                },
            ],
        },
    )
    yield respx_mock


@pytest.fixture
def purge_client(toolkit_config: ToolkitClientConfig) -> Iterator[ToolkitClient]:
    config = toolkit_config
    client = ToolkitClient(config)
    yield client


class TestPurgeInstances:
    @pytest.mark.usefixtures("disable_gzip")
    @pytest.mark.parametrize(
        "dry_run,unlink,instance_type",
        [
            pytest.param(True, True, "timeseries", id="dry run with unlink timeseries"),
            pytest.param(True, True, "files", id="dry run with unlink files"),
            pytest.param(True, False, "timeseries", id="dry run without unlink timeseries"),
            pytest.param(True, False, "files", id="dry run without unlink files"),
            pytest.param(False, True, "timeseries", id="purge with unlink timeseries"),
            pytest.param(False, True, "files", id="purge with unlink files"),
            pytest.param(False, False, "timeseries", id="purge without unlink timeseries"),
            pytest.param(False, False, "files", id="purge without unlink files"),
        ],
    )
    def test_purge(
        self,
        dry_run: bool,
        unlink: bool,
        instance_type: str,
        purge_client: ToolkitClient,
        purge_responses: respx.MockRouter,
        project_statistics_response: dict[str, Any],
        respx_mock: respx.MockRouter,
        cognite_timeseries_2000_list: NodeList[CogniteTimeSeries],
        timeseries_by_node_id: dict[dm.NodeId, dict[str, Any]],
        cognite_files_2000_list: NodeList[CogniteFile],
        files_by_node_id: dict[dm.NodeId, dict[str, Any]],
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        config = purge_client.config
        instances = cognite_timeseries_2000_list if instance_type == "timeseries" else cognite_files_2000_list
        client = purge_client
        monkeypatch.setattr("cognite_toolkit._cdf_tk.commands._purge.questionary", MagicMock())
        monkeypatch.setattr(PurgeCommand, "_confirm_purge", lambda self, msg, client: True)
        if not dry_run:
            respx_mock.get(config.create_api_url("/models/statistics")).respond(
                status_code=200,
                json=project_statistics_response,
            )
        respx_mock.post(config.create_api_url("/models/instances/aggregate")).respond(
            status_code=200,
            json={
                "items": [
                    {
                        "instanceType": "node",
                        "aggregates": [{"aggregate": "count", "value": len(instances), "property": "externalId"}],
                    }
                ]
            },
        )
        instance_dumps = [instance.dump() for instance in instances]
        respx_mock.post(config.create_api_url("/models/instances/query")).side_effect = [
            httpx.Response(
                status_code=200,
                json={"items": {"root": instance_dumps[:1000]}, "nextCursor": {"root": "next"}},
            ),
            httpx.Response(
                status_code=200,
                json={"items": {"root": instance_dumps[1000:]}, "nextCursor": {"root": None}},
            ),
        ]
        if unlink:

            def ts_byids_callback(request: httpx.Request) -> httpx.Response:
                body = json.loads(request.content.decode("utf-8"))
                requested_ids = {
                    dm.NodeId(space=item["instanceId"]["space"], external_id=item["instanceId"]["externalId"])
                    for item in body.get("items", [])
                }
                filtered = [v for k, v in timeseries_by_node_id.items() if k in requested_ids]
                return httpx.Response(status_code=200, json={"items": filtered})

            def files_byids_callback(request: httpx.Request) -> httpx.Response:
                body = json.loads(request.content.decode("utf-8"))
                requested_ids = {
                    dm.NodeId(space=item["instanceId"]["space"], external_id=item["instanceId"]["externalId"])
                    for item in body.get("items", [])
                }
                filtered = [v for k, v in files_by_node_id.items() if k in requested_ids]
                return httpx.Response(status_code=200, json={"items": filtered})

            respx_mock.post(config.create_api_url("/timeseries/byids")).mock(side_effect=ts_byids_callback)
            respx_mock.post(config.create_api_url("/files/byids")).mock(side_effect=files_byids_callback)
        if unlink and not dry_run and instance_type == "timeseries":
            respx_mock.post(config.create_api_url("/timeseries/unlink-instance-ids")).mock(
                return_value=httpx.Response(
                    status_code=200,
                    json={"items": list(timeseries_by_node_id.values())},
                )
            )
        if unlink and not dry_run and instance_type == "files":
            respx_mock.post(config.create_api_url("/files/unlink-instance-ids")).mock(
                return_value=httpx.Response(
                    status_code=200,
                    json={"items": list(files_by_node_id.values())},
                )
            )
        if not dry_run:
            respx_mock.post(
                config.create_api_url("/models/instances/delete"),
            ).mock(
                return_value=httpx.Response(
                    status_code=200,
                    json={"items": [instance.as_id().dump() for instance in instances]},
                )
            )

        cmd = PurgeCommand(silent=True)
        result = cmd.instances(
            client,
            InstanceViewSelector(view=SelectedView(space="cdf_cdm", external_id="CogniteTimeSeries", version="v1")),
            log_dir=tmp_path,
            dry_run=dry_run,
            unlink=unlink,
            verbose=False,
        )
        assert result.deleted == 2000


class TestPurgeSpace:
    @pytest.mark.usefixtures("disable_gzip")
    @pytest.mark.parametrize(
        "dry_run, include_space,delete_datapoints,delete_file_content,",
        itertools.product([True, False], [True, False], [True, False], [True, False]),
    )
    def test_purge(
        self,
        dry_run: bool,
        include_space: bool,
        delete_datapoints: bool,
        delete_file_content: bool,
        purge_client: ToolkitClient,
        purge_responses: respx.MockRouter,
        project_statistics_response: dict[str, Any],
        respx_mock: respx.MockRouter,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        config = purge_client.config
        space = "test_space"
        monkeypatch.setattr("cognite_toolkit._cdf_tk.commands._purge.questionary", MagicMock())
        monkeypatch.setattr(PurgeCommand, "_confirm_purge", lambda self, msg, client: True)
        container_count = 10
        view_count = 15
        data_model_count = 3
        edge_count = 40
        node_count = 50
        ts_count = 14
        file_count = 7
        respx_mock.post(config.create_api_url("/models/statistics/spaces/byids")).respond(
            status_code=200,
            json={
                "items": SpaceStatistics(
                    space, container_count, view_count, data_model_count, edge_count, 0, node_count, 0
                ).dump()
            },
        )
        if not dry_run:
            respx_mock.get(config.create_api_url("/models/statistics")).respond(
                status_code=200,
                json=project_statistics_response,
            )

        def delete_callback(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=request.content)

        gen = FakeCogniteResourceGenerator(seed=42)
        # The cross-reference safety check runs in both dry-run and real mode and lists/inspects all
        # containers in the space. Mock both endpoints with no external view references.
        container_items = [gen.create_instance(ContainerResponse) for _ in range(container_count)]
        respx_mock.get(config.create_api_url("/models/containers")).respond(
            status_code=200, json={"items": [item.dump() for item in container_items]}
        )
        respx_mock.post(config.create_api_url("/models/containers/inspect")).respond(
            status_code=200,
            json={
                "items": [
                    {
                        "space": item.space,
                        "externalId": item.external_id,
                        "inspectionResults": {"involvedViewCount": 0, "involvedViews": []},
                    }
                    for item in container_items
                ]
            },
        )

        if not dry_run:
            space_obj = gen.create_instance(Space)
            space_obj.space = space
            delete_urls = [
                "/models/containers/delete",
                "/models/views/delete",
                "/models/datamodels/delete",
                "/models/instances/delete",
            ]
            for url in delete_urls:
                respx_mock.post(config.create_api_url(url)).mock(side_effect=delete_callback)
            if include_space:
                respx_mock.post(config.create_api_url("/models/spaces/delete")).mock(side_effect=delete_callback)

            list_calls = [
                ("/models/views", "GET", ViewResponse, view_count),
                ("/models/datamodels", "GET", DataModelResponse, data_model_count),
            ]
            if include_space:
                list_calls.append(("/models/spaces/byids", "POST", SpaceResponse, 1))
            for url, method, cls_, count in list_calls:
                items = [gen.create_instance(cls_) for _ in range(count)]
                respx_mock.request(method=method, url=config.create_api_url(url)).respond(
                    status_code=200, json={"items": [item.dump() for item in items]}
                )
            edge_items = [gen.create_instance(EdgeResponse) for _ in range(edge_count)]
            node_items = [gen.create_instance(NodeResponse) for _ in range(node_count)]

            def list_instances_query_callback(request: httpx.Request) -> httpx.Response:
                body = json.loads(request.content.decode("utf-8"))
                root_expr = body.get("with", {}).get("root", {})
                items = edge_items if "edges" in root_expr else node_items
                return httpx.Response(
                    200,
                    json={"items": {"root": [item.dump() for item in items]}, "nextCursor": {"root": None}},
                )

            respx_mock.post(config.create_api_url("/models/instances/query")).mock(
                side_effect=list_instances_query_callback
            )

            nodes = node_items
            retrieve_calls: list[tuple[str, type, int, list[NodeResponse]]] = []
            if not delete_datapoints:
                retrieve_calls.append(("/timeseries/byids", TimeSeriesResponse, ts_count, nodes[:ts_count]))
            if not delete_file_content:
                retrieve_calls.append(
                    ("/files/byids", FileMetadataResponse, file_count, nodes[ts_count : ts_count + file_count])
                )

            for url, cls_, count, resource_nodes in retrieve_calls:
                resource_list = [gen.create_instance(cls_).dump() for _ in range(count)]
                for ts, node in zip(resource_list, resource_nodes):
                    try:
                        ts["instanceId"] = node.as_id().dump(include_type=False)
                    except TypeError:
                        ts["instanceId"] = node.as_id().dump(camel_case=True)
                respx_mock.post(config.create_api_url(url)).respond(json={"items": resource_list})

        cmd = PurgeCommand(silent=True)
        results = cmd.space(
            purge_client,
            "test_space",
            include_space=include_space,
            delete_datapoints=delete_datapoints,
            delete_file_content=delete_file_content,
            dry_run=dry_run,
            verbose=False,
            log_dir=tmp_path,
        )
        expected_node_count = (
            node_count
            - (0 if delete_datapoints or dry_run else ts_count)
            - (0 if delete_file_content or dry_run else file_count)
        )
        expected = {
            "containers": container_count,
            "views": view_count,
            "data models": data_model_count,
            "edges": edge_count,
            "nodes": expected_node_count,
        }
        if include_space:
            expected["spaces"] = 1

        assert {name: value.deleted for name, value in results.data.items()} == expected


class TestPurgeSpaceCrossReferenceCheck:
    @pytest.mark.parametrize("dry_run", [True, False])
    def test_blocks_when_external_view_references_container(
        self,
        dry_run: bool,
        purge_client: ToolkitClient,
        respx_mock: respx.MockRouter,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        config = purge_client.config
        space = "test_space"
        monkeypatch.setattr("cognite_toolkit._cdf_tk.commands._purge.questionary", MagicMock())
        monkeypatch.setattr(PurgeCommand, "_confirm_purge", lambda self, msg, client: True)

        respx_mock.post(config.create_api_url("/models/statistics/spaces/byids")).respond(
            status_code=200,
            json={"items": SpaceStatistics(space, 1, 0, 0, 0, 0, 0, 0).dump()},
        )

        gen = FakeCogniteResourceGenerator(seed=1)
        container = gen.create_instance(ContainerResponse)
        container.space = space
        container.external_id = "blocked_container"
        respx_mock.get(config.create_api_url("/models/containers")).respond(
            status_code=200, json={"items": [container.dump()]}
        )
        respx_mock.post(config.create_api_url("/models/containers/inspect")).respond(
            status_code=200,
            json={
                "items": [
                    {
                        "space": space,
                        "externalId": container.external_id,
                        "inspectionResults": {
                            "involvedViewCount": 1,
                            "involvedViews": [
                                {
                                    "type": "view",
                                    "space": "other_space",
                                    "externalId": "ExternalView",
                                    "version": "v1",
                                }
                            ],
                        },
                    }
                ]
            },
        )
        delete_route = respx_mock.post(config.create_api_url("/models/containers/delete"))

        cmd = PurgeCommand(silent=True)
        with pytest.raises(ToolkitValueError, match="Cannot proceed with purge"):
            cmd.space(purge_client, space, log_dir=tmp_path, dry_run=dry_run)
        assert delete_route.call_count == 0

    @pytest.mark.parametrize("dry_run", [True, False])
    def test_blocks_when_hidden_views_reference_container(
        self,
        dry_run: bool,
        purge_client: ToolkitClient,
        respx_mock: respx.MockRouter,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """involvedViewCount > len(involvedViews) means inaccessible views exist; should still block."""
        config = purge_client.config
        space = "test_space"
        monkeypatch.setattr("cognite_toolkit._cdf_tk.commands._purge.questionary", MagicMock())
        monkeypatch.setattr(PurgeCommand, "_confirm_purge", lambda self, msg, client: True)

        respx_mock.post(config.create_api_url("/models/statistics/spaces/byids")).respond(
            status_code=200,
            json={"items": SpaceStatistics(space, 1, 0, 0, 0, 0, 0, 0).dump()},
        )

        gen = FakeCogniteResourceGenerator(seed=2)
        container = gen.create_instance(ContainerResponse)
        container.space = space
        container.external_id = "blocked_container"
        respx_mock.get(config.create_api_url("/models/containers")).respond(
            status_code=200, json={"items": [container.dump()]}
        )
        # involvedViewCount=3 but involvedViews is empty — caller has no access to any referencing views
        respx_mock.post(config.create_api_url("/models/containers/inspect")).respond(
            status_code=200,
            json={
                "items": [
                    {
                        "space": space,
                        "externalId": container.external_id,
                        "inspectionResults": {"involvedViewCount": 3, "involvedViews": []},
                    }
                ]
            },
        )
        delete_route = respx_mock.post(config.create_api_url("/models/containers/delete"))

        cmd = PurgeCommand(silent=True)
        with pytest.raises(ToolkitValueError, match="Cannot proceed with purge"):
            cmd.space(purge_client, space, log_dir=tmp_path, dry_run=dry_run)
        assert delete_route.call_count == 0

    def test_does_not_block_when_only_same_space_views_reference_container(
        self,
        purge_client: ToolkitClient,
        respx_mock: respx.MockRouter,
    ) -> None:
        """Same-space views are part of the purge and must not trigger the guardrail."""
        config = purge_client.config
        space = "test_space"

        gen = FakeCogniteResourceGenerator(seed=3)
        container = gen.create_instance(ContainerResponse)
        container.space = space
        container.external_id = "same_space_container"
        respx_mock.get(config.create_api_url("/models/containers")).respond(
            status_code=200, json={"items": [container.dump()]}
        )
        respx_mock.post(config.create_api_url("/models/containers/inspect")).respond(
            status_code=200,
            json={
                "items": [
                    {
                        "space": space,
                        "externalId": container.external_id,
                        "inspectionResults": {
                            "involvedViewCount": 1,
                            "involvedViews": [
                                {
                                    "type": "view",
                                    "space": space,
                                    "externalId": "SameSpaceView",
                                    "version": "v1",
                                }
                            ],
                        },
                    }
                ]
            },
        )

        # Should not raise — same-space view is part of the purge
        PurgeCommand._block_if_external_views_reference_containers(purge_client, space)


class TestSoftDeletePurgeHeadroom:
    def test_validate_blocks_when_headroom_below_margin(self) -> None:
        inst_stats = InstanceStatistics(
            nodes=1000,
            edges=0,
            soft_deleted_edges=0,
            soft_deleted_nodes=0,
            instances_limit=10_000_000,
            soft_deleted_instances_limit=10_000_000,
            instances=1000,
            soft_deleted_instances=9_200_000,
        )
        with pytest.raises(ToolkitValueError, match="Cannot proceed"):
            validate_soft_delete_purge_headroom(inst_stats, 900_000, action="test purge")

    def test_validate_ok_when_headroom_sufficient(self) -> None:
        inst_stats = InstanceStatistics(
            nodes=1000,
            edges=0,
            soft_deleted_edges=0,
            soft_deleted_nodes=0,
            instances_limit=10_000_000,
            soft_deleted_instances_limit=10_000_000,
            instances=1000,
            soft_deleted_instances=100,
        )
        validate_soft_delete_purge_headroom(inst_stats, 2000, action="test purge")
