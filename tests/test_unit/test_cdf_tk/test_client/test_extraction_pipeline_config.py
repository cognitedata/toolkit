import json

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.identifiers import ExtractionPipelineConfigId


@pytest.mark.usefixtures("disable_gzip")
class TestExtractionPipelineConfigsAPI:
    def test_revert(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.post(toolkit_config.create_api_url("/extpipes/config/revert")).mock(
            return_value=httpx2.Response(200, json={"externalId": "my_pipeline", "revision": 3, "createdTime": 0})
        )

        toolkit_client.extraction_pipelines.configs.revert(
            ExtractionPipelineConfigId(external_id="my_pipeline", revision=2)
        )

        assert json.loads(route.calls[0].request.content) == {"externalId": "my_pipeline", "revision": 2}
