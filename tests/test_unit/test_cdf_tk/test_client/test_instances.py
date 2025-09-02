import json

import pytest
import responses
from cognite.client.data_classes.data_modeling import (
    Edge,
    EdgeApply,
    EdgeId,
    Node,
    NodeApply,
    NodeId,
    NodeOrEdgeData,
    ViewId,
)
from cognite.client.data_classes.data_modeling.cdm.v1 import (
    CogniteAnnotationApply,
    CogniteAssetApply,
    CogniteTimeSeriesApply,
)
from cognite.client.exceptions import CogniteAPIError, CogniteConnectionError, CogniteReadTimeout

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.data_classes.instances import InstanceList


@pytest.fixture()
def some_timeseries() -> CogniteTimeSeriesApply:
    return CogniteTimeSeriesApply(
        space="some_space",
        external_id="test_time_series",
        is_step=False,
        time_series_type="numeric",
    )


class TestInstances:
    def test_apply_fast_failed_multiple_types(self, toolkit_config) -> None:
        different_instance_types = [
            CogniteTimeSeriesApply(
                space="some_space",
                external_id="test_time_series",
                is_step=False,
                time_series_type="numeric",
            ),
            CogniteAssetApply(
                space="some_space",
                external_id="test_asset",
                name="Test Asset",
            ),
            CogniteAnnotationApply(
                space="some_space",
                external_id="test_annotation",
                type=("some_space", "entity.match"),
                start_node=("some_space", "test_asset"),
                end_node=("some_space", "test_asset"),
                name="Test Annotation",
            ),
        ]
        url = f"{toolkit_config.base_url}/api/v1/projects/test-project/models/instances"
        with responses.RequestsMock() as rsps:
            rsps.add(
                responses.POST,
                url,
                status=400,
                json={"error": "Invalid request", "message": "the message"},
            )
            client = ToolkitClient(config=toolkit_config)
            with pytest.raises(CogniteAPIError) as exc_info:
                _ = client.data_modeling.instances.apply_fast(different_instance_types)

        error = exc_info.value
        assert isinstance(error, CogniteAPIError)
        assert error.code == 400
        assert error.message == "Invalid request"
        assert error.failed == [instance.as_id() for instance in different_instance_types]

    @pytest.mark.usefixtures("disable_gzip")
    def test_apply_fast_429_status_split(self, toolkit_config: ToolkitClientConfig) -> None:
        instances = [
            CogniteTimeSeriesApply(
                space="some_space",
                external_id=f"test_time_series_{i}",
                is_step=False,
                time_series_type="numeric",
            )
            for i in range(2)
        ]
        url = f"{toolkit_config.base_url}/api/v1/projects/test-project/models/instances"
        with responses.RequestsMock() as rsps:
            rsps.add(
                responses.POST,
                url,
                status=429,
                json={"error": "Too many items"},
            )
            for ts in instances:
                rsps.add(
                    responses.POST,
                    url,
                    status=200,
                    json={
                        "items": [
                            {
                                "instanceType": "node",
                                "space": ts.space,
                                "externalId": ts.external_id,
                                "version": 1,
                                "wasModified": False,
                                "createdTime": 0,
                                "lastUpdatedTime": 0,
                            }
                        ]
                    },
                )
            client = ToolkitClient(config=toolkit_config)

            results = client.data_modeling.instances.apply_fast(instances)

            assert len(results) == 2
            assert len(rsps.calls) >= 3
            failed_call = rsps.calls[-3]
            assert failed_call.response.status_code == 429
            assert len(json.loads(failed_call.request.body)["items"]) == 2
            for call in rsps.calls[-2:]:
                assert call.response.status_code == 200
                assert len(json.loads(call.request.body)["items"]) == 1

    @pytest.mark.usefixtures("disable_gzip")
    def test_apply_fast_429_single_instance(
        self, toolkit_config: ToolkitClientConfig, some_timeseries: CogniteTimeSeriesApply
    ) -> None:
        url = f"{toolkit_config.base_url}/api/v1/projects/test-project/models/instances"
        with responses.RequestsMock() as rsps:
            rsps.add(
                responses.POST,
                url,
                status=429,
                json={"error": "Too many items"},
            )
            rsps.add(
                responses.POST,
                url,
                status=200,
                json={
                    "items": [
                        {
                            "instanceType": "node",
                            "space": some_timeseries.space,
                            "externalId": some_timeseries.external_id,
                            "version": 1,
                            "wasModified": False,
                            "createdTime": 0,
                            "lastUpdatedTime": 0,
                        }
                    ]
                },
            )
            client = ToolkitClient(config=toolkit_config)

            results = client.data_modeling.instances.apply_fast([some_timeseries])

            assert len(results) == 1
            assert len(rsps.calls) >= 2
            failed_call = rsps.calls[-2]
            assert failed_call.response.status_code == 429
            assert len(json.loads(failed_call.request.body)["items"]) == 1
            last_call = rsps.calls[-1]
            assert last_call.response.status_code == 200
            assert len(json.loads(last_call.request.body)["items"]) == 1

    @pytest.mark.usefixtures("max_retries_2")
    @pytest.mark.parametrize(
        "args, expected_exception",
        [
            pytest.param(
                dict(status=429, json={"error": "Server is busy, please try again later"}),
                CogniteAPIError,
                id="429 Too Many Requests",
            ),
            pytest.param(
                dict(body=ConnectionRefusedError("Connection refused")), CogniteConnectionError, id="Connection refused"
            ),
            pytest.param(dict(body=TimeoutError("timed out")), CogniteReadTimeout, id="Connection timed out"),
        ],
    )
    def test_apply_fast_raise(
        self,
        args: dict[str, object],
        expected_exception: type[Exception],
        toolkit_config: ToolkitClientConfig,
        some_timeseries: CogniteTimeSeriesApply,
    ) -> None:
        url = f"{toolkit_config.base_url}/api/v1/projects/test-project/models/instances"
        with responses.RequestsMock() as rsps:
            rsps.add(
                responses.POST,
                url,
                **args,
            )
            client = ToolkitClient(config=toolkit_config)
            with pytest.raises(expected_exception) as _:
                _ = client.data_modeling.instances.apply_fast([some_timeseries])

    def test_apply_fast_invalid_json(self, toolkit_config: ToolkitClientConfig) -> None:
        node = NodeApply(
            space="some_space",
            external_id="test_time_series",
            sources=[
                NodeOrEdgeData(source=ViewId("some_space", "some_view", "v1"), properties={"my_number": float("nan")})
            ],
        )
        client = ToolkitClient(config=toolkit_config)

        with pytest.raises(ValueError) as exc_info:
            _ = client.data_modeling.instances.apply_fast([node])

        assert "Out of range float values are not JSON compliant." in str(exc_info.value)

    def test_apply_fast_empty(self, toolkit_config: ToolkitClientConfig) -> None:
        client = ToolkitClient(config=toolkit_config)
        result = client.data_modeling.instances.apply_fast([])
        assert len(result) == 0

    @pytest.mark.usefixtures("disable_gzip")
    def test_delete_fast_400(self, toolkit_config: ToolkitClientConfig) -> None:
        url = f"{toolkit_config.base_url}/api/v1/projects/test-project/models/instances/delete"
        error_message = "This will trigger a 400 error. 287891n-unique"
        ids = [NodeId("node-space", "node1"), EdgeId("edge-space", "edge1")]
        with responses.RequestsMock() as rsps:
            rsps.add(
                responses.POST,
                url,
                status=400,
                json={"error": error_message},
            )
            client = ToolkitClient(config=toolkit_config)

            with pytest.raises(CogniteAPIError) as exc_info:
                _ = client.data_modeling.instances.delete_fast(ids)

            error = exc_info.value
            assert isinstance(error, CogniteAPIError)
            assert error.code == 400
            assert error.message == error_message
            assert error.failed == ids


class TestInstancesDataClasses:
    def test_instance_list_mixed_instance_types(self) -> None:
        node = Node(
            space="some_space",
            external_id="test_node",
            version=1,
            created_time=0,
            last_updated_time=0,
            deleted_time=None,
            type=None,
            properties=None,
        )
        edge = Edge(
            space="some_space",
            external_id="test_edge",
            version=1,
            created_time=0,
            last_updated_time=0,
            deleted_time=None,
            type=("some_space", "some_relation"),
            properties=None,
            start_node=("some_space", "start_node"),
            end_node=("some_space", "end_node"),
        )
        my_list = InstanceList([node, edge])
        assert len(my_list) == 2
        my_write_list = my_list.as_write()
        assert len(my_write_list) == 2
        assert isinstance(my_write_list[0], NodeApply)
        assert isinstance(my_write_list[1], EdgeApply)
