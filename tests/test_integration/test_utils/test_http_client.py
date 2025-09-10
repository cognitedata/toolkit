from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient, ParamRequest, SimpleBodyRequest, SuccessResponse


class TestHttpClient:
    def test_get_request(self, toolkit_client_config: ToolkitClientConfig) -> None:
        config = toolkit_client_config
        with HTTPClient(config) as client:
            result = client.request(
                ParamRequest(
                    endpoint_url=config.create_api_url(""),
                    method="GET",
                )
            )

        assert len(result) == 1
        response = result[0]
        assert isinstance(response, SuccessResponse)
        assert response.status_code == 200

    def test_post_request(self, toolkit_client_config: ToolkitClientConfig) -> None:
        config = toolkit_client_config
        with HTTPClient(config) as client:
            result = client.request(
                SimpleBodyRequest(
                    endpoint_url=config.create_api_url("timeseries/list"), method="POST", body_content={"limit": 1}
                )
            )
        assert len(result) == 1
        response = result[0]
        assert isinstance(response, SuccessResponse)
        assert response.status_code == 200
        response_body = response.body
        assert "items" in response_body
        assert isinstance(response_body["items"], list)
        assert len(response_body["items"]) <= 1
