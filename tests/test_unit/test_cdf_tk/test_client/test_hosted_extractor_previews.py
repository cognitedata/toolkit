import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_preview import HostedExtractorPreviewPayload


@pytest.mark.usefixtures("disable_gzip")
class TestHostedExtractorPreviewsAPI:
    def test_result_is_parsed_by_type(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        respx_mock.post(toolkit_config.create_api_url("/hostedextractors/preview/result")).mock(
            return_value=httpx2.Response(
                200, json={"type": "preview", "externalId": "my_preview", "json": {"value": 1}, "createdTime": 0}
            )
        )

        result = toolkit_client.hosted_extractors.previews.result("my_preview")

        assert result == HostedExtractorPreviewPayload(external_id="my_preview", payload={"value": 1}, created_time=0)
