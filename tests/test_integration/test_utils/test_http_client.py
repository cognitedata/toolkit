import json

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, RequestMessage2, SuccessResponse2


class TestHttpClient:
    def test_get_request(self, toolkit_client_config: ToolkitClientConfig) -> None:
        config = toolkit_client_config
        with HTTPClient(config) as client:
            response = client.request_single(
                RequestMessage2(
                    endpoint_url=config.create_api_url(""),
                    method="GET",
                )
            )

        assert isinstance(response, SuccessResponse2)
        assert response.status_code == 200

    def test_post_request(self, toolkit_client_config: ToolkitClientConfig) -> None:
        config = toolkit_client_config
        with HTTPClient(config) as client:
            response = client.request_single(
                RequestMessage2(
                    endpoint_url=config.create_api_url("timeseries/list"), method="POST", body_content={"limit": 1}
                )
            )
        assert isinstance(response, SuccessResponse2)
        assert response.status_code == 200
        response_body = json.loads(response.body)
        assert "items" in response_body
        assert isinstance(response_body["items"], list)
        assert len(response_body["items"]) <= 1
