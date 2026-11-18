import json

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.identifiers import NodeId, ThreeDJobId

_JOB = ThreeDJobId(space="my_space", external_id="my_job")
_RESULT = NodeId(space="my_space", external_id="my_result")


@pytest.mark.usefixtures("disable_gzip")
class TestThreeDJobsAPI:
    def test_update_rejections(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.post(toolkit_config.create_api_url("/3d/job/result/rejections")).mock(
            return_value=httpx2.Response(200, json={})
        )

        toolkit_client.three_d.jobs.update_rejections(_JOB, _RESULT, add=["key1"])

        assert json.loads(route.calls[0].request.content) == {
            "job": {"space": "my_space", "externalId": "my_job"},
            "result": {"instanceId": {"space": "my_space", "externalId": "my_result"}},
            "add": ["key1"],
        }

    def test_update_rejections_requires_keys(self, toolkit_client: ToolkitClient) -> None:
        with pytest.raises(ValueError, match="at least one key"):
            toolkit_client.three_d.jobs.update_rejections(_JOB, _RESULT)

    def test_list_rejections_truncated_to_limit(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        respx_mock.post(toolkit_config.create_api_url("/3d/job/result/rejections/list")).mock(
            side_effect=[
                httpx2.Response(200, json={"items": [{"rejectedKey": "k1"}], "nextCursor": "c1"}),
                httpx2.Response(200, json={"items": [{"rejectedKey": "k2"}, {"rejectedKey": "k3"}]}),
            ]
        )

        assert toolkit_client.three_d.jobs.list_rejections(_JOB, _RESULT, limit=2) == ["k1", "k2"]


@pytest.mark.usefixtures("disable_gzip")
def test_asset_mappings_list_by_asset(
    toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
) -> None:
    route = respx_mock.get(toolkit_config.create_api_url("/3d/mappings/123/modelnodes")).mock(
        return_value=httpx2.Response(200, json={"items": []})
    )

    toolkit_client.three_d.asset_mappings_classic.list_by_asset(123, limit=10)

    assert dict(route.calls[0].request.url.params) == {"limit": "10"}
