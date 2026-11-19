import json

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig


@pytest.mark.usefixtures("disable_gzip")
class TestAnnotationsAPI:
    def test_reverse_lookup(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.post(toolkit_config.create_api_url("/annotations/reverselookup")).mock(
            return_value=httpx2.Response(200, json={"items": [{"id": 1}]})
        )

        toolkit_client.annotations.reverse_lookup("file", annotation_type="images.AssetLink")

        assert json.loads(route.calls[0].request.content) == {
            "filter": {"annotatedResourceType": "file", "annotationType": "images.AssetLink"},
            "limit": 100,
        }
