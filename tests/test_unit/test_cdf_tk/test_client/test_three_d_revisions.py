import json

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.resource_classes.three_d import ThreeDRevisionClassicRequest


@pytest.mark.usefixtures("disable_gzip")
class TestThreeDClassicRevisionsAPI:
    def test_update_thumbnail(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.post(toolkit_config.create_api_url("/3d/models/1/revisions/2/thumbnail")).mock(
            return_value=httpx2.Response(200, json={})
        )

        toolkit_client.three_d.revisions_classic.update_thumbnail(1, 2, file_id=3)

        assert json.loads(route.calls[0].request.content) == {"fileId": 3}

    def test_list_logs(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.get(toolkit_config.create_api_url("/3d/models/1/revisions/2/logs")).mock(
            return_value=httpx2.Response(200, json={"items": []})
        )

        toolkit_client.three_d.revisions_classic.list_logs(1, 2, severity=7)

        assert dict(route.calls[0].request.url.params) == {"severity": "7"}


def test_revision_create_only_fields_not_updated() -> None:
    revision = ThreeDRevisionClassicRequest(
        model_id=1,
        id=2,
        file_id=3,
        nonce="my_nonce",
        pipeline_configuration={"indexingOptions": {}},
        published=True,
    )

    assert revision.as_update(mode="replace")["update"].keys() & {"fileId", "nonce", "pipelineConfiguration"} == set()
