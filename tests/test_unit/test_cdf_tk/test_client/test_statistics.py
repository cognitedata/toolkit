import gzip
import json

import httpx2
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.api.statistics import StatisticsAPI
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient
from cognite_toolkit._cdf_tk.client.identifiers import SpaceId
from cognite_toolkit._cdf_tk.client.resource_classes.statistics import (
    CountLimit,
    InstanceStatistics,
    ProjectStatistics,
    SpaceStatisticsResponse,
)

_RESPONSE = {
    "spaces": {"count": 5, "limit": 100},
    "containers": {"count": 42, "limit": 1000},
    "views": {"count": 123, "limit": 2000},
    "dataModels": {"count": 8, "limit": 500},
    "containerProperties": {"count": 1234, "limit": 100},
    "instances": {
        "edges": 5000,
        "softDeletedEdges": 100,
        "nodes": 10000,
        "softDeletedNodes": 200,
        "instances": 15000,
        "instancesLimit": 5000000,
        "softDeletedInstances": 300,
        "softDeletedInstancesLimit": 10000000,
    },
    "concurrentReadLimit": 10,
    "concurrentWriteLimit": 5,
    "concurrentDeleteLimit": 3,
    "recordsOnlyContainers": {"count": 1, "limit": 10},
    "recordsOnlyContainerProperties": {"count": 2, "limit": 20},
}


def test_retrieve_project_statistics(toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter) -> None:
    url = toolkit_config.create_api_url("/models/statistics")
    respx_mock.get(url).mock(return_value=httpx2.Response(status_code=200, json=_RESPONSE))

    stats = StatisticsAPI(HTTPClient(toolkit_config)).retrieve()

    assert stats == ProjectStatistics(
        spaces=CountLimit(count=5, limit=100),
        containers=CountLimit(count=42, limit=1000),
        views=CountLimit(count=123, limit=2000),
        data_models=CountLimit(count=8, limit=500),
        container_properties=CountLimit(count=1234, limit=100),
        instances=InstanceStatistics(
            edges=5000,
            soft_deleted_edges=100,
            nodes=10000,
            soft_deleted_nodes=200,
            instances=15000,
            instances_limit=5000000,
            soft_deleted_instances=300,
            soft_deleted_instances_limit=10000000,
        ),
        concurrent_read_limit=10,
        concurrent_write_limit=5,
        concurrent_delete_limit=3,
        records_only_containers=CountLimit(count=1, limit=10),
        records_only_container_properties=CountLimit(count=2, limit=20),
    )


_SPACE_WITH_OPTIONAL_COUNTS = {
    "space": "my_space",
    "containers": 2,
    "views": 3,
    "dataModels": 1,
    "edges": 4,
    "softDeletedEdges": 5,
    "nodes": 6,
    "softDeletedNodes": 7,
    "containerProperties": 8,
    "recordsOnlyContainers": 9,
    "recordsOnlyContainerProperties": 10,
}

_SPACE_REQUIRED_COUNTS_ONLY = {
    "space": "other_space",
    "containers": 0,
    "views": 1,
    "dataModels": 0,
    "edges": 2,
    "softDeletedEdges": 0,
    "nodes": 3,
    "softDeletedNodes": 0,
}


def test_list_space_statistics(toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter) -> None:
    url = toolkit_config.create_api_url("/models/statistics/spaces")
    respx_mock.get(url).mock(
        return_value=httpx2.Response(
            status_code=200,
            json={"items": [_SPACE_WITH_OPTIONAL_COUNTS, _SPACE_REQUIRED_COUNTS_ONLY]},
        )
    )

    stats = StatisticsAPI(HTTPClient(toolkit_config)).spaces.list()

    assert stats == [
        SpaceStatisticsResponse(
            space="my_space",
            containers=2,
            views=3,
            data_models=1,
            edges=4,
            soft_deleted_edges=5,
            nodes=6,
            soft_deleted_nodes=7,
            container_properties=8,
            records_only_containers=9,
            records_only_container_properties=10,
        ),
        SpaceStatisticsResponse(
            space="other_space",
            containers=0,
            views=1,
            data_models=0,
            edges=2,
            soft_deleted_edges=0,
            nodes=3,
            soft_deleted_nodes=0,
        ),
    ]


def test_retrieve_space_statistics_by_ids(toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter) -> None:
    url = toolkit_config.create_api_url("/models/statistics/spaces/byids")
    respx_mock.post(url).mock(
        return_value=httpx2.Response(status_code=200, json={"items": [_SPACE_WITH_OPTIONAL_COUNTS]})
    )

    stats = StatisticsAPI(HTTPClient(toolkit_config)).spaces.retrieve([SpaceId(space="my_space")])

    assert (stats, json.loads(gzip.decompress(respx_mock.calls.last.request.content))) == (
        [
            SpaceStatisticsResponse(
                space="my_space",
                containers=2,
                views=3,
                data_models=1,
                edges=4,
                soft_deleted_edges=5,
                nodes=6,
                soft_deleted_nodes=7,
                container_properties=8,
                records_only_containers=9,
                records_only_container_properties=10,
            )
        ],
        {"items": [{"space": "my_space"}]},
    )
