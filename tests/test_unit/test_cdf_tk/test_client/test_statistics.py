import httpx2
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.api.statistics import StatisticsAPI
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient
from cognite_toolkit._cdf_tk.client.resource_classes.statistics import CountLimit, InstanceStatistics, ProjectStatistics

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
