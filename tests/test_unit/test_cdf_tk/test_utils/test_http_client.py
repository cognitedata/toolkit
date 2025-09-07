import json
from collections.abc import Iterator

import pytest
import responses

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient, ParamRequest, SimpleBodyRequest, SuccessResponse


@pytest.fixture
def rsps() -> Iterator[responses.RequestsMock]:
    with responses.RequestsMock() as rsps:
        yield rsps


@pytest.fixture
def http_client(toolkit_config: ToolkitClientConfig) -> Iterator[HTTPClient]:
    with HTTPClient(toolkit_config) as client:
        yield client


class TestHTTPClient:
    def test_get_request(self, rsps: responses.RequestsMock, http_client: HTTPClient) -> None:
        rsps.get("https://example.com/api/resource", json={"key": "value"}, status=200)
        results = http_client.request(
            ParamRequest(endpoint_url="https://example.com/api/resource", method="GET", parameters={"query": "test"})
        )
        assert len(results) == 1
        response = results[0]
        assert isinstance(response, SuccessResponse)
        assert response.status_code == 200
        assert response.body == {"key": "value"}
        assert rsps.calls[-1].request.url == "https://example.com/api/resource?query=test"

    @pytest.mark.usefixtures("disable_gzip")
    def test_post_request(self, rsps: responses.RequestsMock, http_client: HTTPClient) -> None:
        rsps.post("https://example.com/api/resource", json={"id": 123, "status": "created"}, status=201)
        results = http_client.request(
            SimpleBodyRequest(
                endpoint_url="https://example.com/api/resource", method="POST", body_content={"name": "new resource"}
            )
        )
        assert len(results) == 1
        response = results[0]
        assert isinstance(response, SuccessResponse)
        assert response.status_code == 201
        assert response.body == {"id": 123, "status": "created"}
        assert rsps.calls[-1].request.body == json.dumps({"name": "new resource"})
