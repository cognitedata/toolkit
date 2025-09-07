import json
from collections.abc import Iterator
from unittest.mock import patch

import pytest
import requests
import responses

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.utils.http_client import (
    FailedRequestMessage,
    FailedResponse,
    HTTPClient,
    ParamRequest,
    SimpleBodyRequest,
    SuccessResponse,
)
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal


@pytest.fixture
def rsps() -> Iterator[responses.RequestsMock]:
    with responses.RequestsMock() as rsps:
        yield rsps


@pytest.fixture
def http_client(toolkit_config: ToolkitClientConfig) -> Iterator[HTTPClient]:
    with HTTPClient(toolkit_config) as client:
        yield client


@pytest.fixture
def http_client_one_retry(toolkit_config: ToolkitClientConfig) -> Iterator[HTTPClient]:
    with HTTPClient(toolkit_config, max_retries=1) as client:
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

    @pytest.mark.usefixtures("disable_gzip")
    @pytest.mark.parametrize(
        "bad_body,error",
        [
            pytest.param({"name": bytes(10)}, "can't be serialized by the JSON encoder", id="bytes value"),
            pytest.param(
                {"values": [float("nan")], "other": float("inf")},
                "Out of range float values are not JSON compliant",
                id="nan and inf",
            ),
        ],
    )
    def test_post_request_bad_json(self, bad_body: dict[str, JsonVal], error: str, http_client: HTTPClient) -> None:
        results = http_client.request(
            SimpleBodyRequest(
                endpoint_url="https://example.com/api/resource",
                method="POST",
                body_content=bad_body,
            )
        )
        assert len(results) == 1
        response = results[0]
        assert isinstance(response, FailedRequestMessage)
        assert error in response.error

    def test_failed_request(self, rsps: responses.RequestsMock, http_client: HTTPClient) -> None:
        rsps.get("https://example.com/api/resource", json={"error": "bad request"}, status=400)
        results = http_client.request(
            ParamRequest(endpoint_url="https://example.com/api/resource", method="GET", parameters={"query": "fail"})
        )
        assert len(results) == 1
        response = results[0]
        assert isinstance(response, FailedResponse)
        assert response.status_code == 400
        assert response.error == "bad request"

    @pytest.mark.usefixtures("disable_gzip")
    def test_retry_then_success(self, rsps: responses.RequestsMock, http_client: HTTPClient) -> None:
        url = "https://example.com/api/resource"
        rsps.get(url, json={"error": "service unavailable"}, status=503)
        rsps.get(url, json={"key": "value"}, status=200)
        results = http_client.request_with_retries(ParamRequest(endpoint_url=url, method="GET"))
        assert len(results) == 1
        response = results[0]
        assert isinstance(response, SuccessResponse)
        assert response.status_code == 200
        assert response.body == {"key": "value"}

    def test_retry_exhausted(self, http_client_one_retry: HTTPClient, rsps: responses.RequestsMock) -> None:
        client = http_client_one_retry
        for _ in range(2):
            rsps.get("https://example.com/api/resource", json={"error": {"message": "service unavailable"}}, status=503)
        with patch("time.sleep"):  # Patch sleep to speed up the test
            results = client.request_with_retries(
                ParamRequest(endpoint_url="https://example.com/api/resource", method="GET")
            )

        assert len(results) == 1
        response = results[0]
        assert isinstance(response, FailedResponse)
        assert response.status_code == 503
        assert response.error == "service unavailable"

    def test_invalid_json_response(self, rsps: responses.RequestsMock, http_client: HTTPClient) -> None:
        rsps.get("https://example.com/api/resource", body="not json", status=200)
        results = http_client.request(ParamRequest(endpoint_url="https://example.com/api/resource", method="GET"))
        assert len(results) == 1
        response = results[0]
        assert isinstance(response, FailedResponse)
        assert response.status_code == 200
        assert "Invalid JSON response" in response.error

    def test_connection_error(self, http_client_one_retry: HTTPClient, rsps: responses.RequestsMock) -> None:
        http_client = http_client_one_retry
        rsps.add(
            responses.GET,
            "http://nonexistent.domain/api/resource",
            body=requests.ConnectionError("Simulated connection error"),
        )
        results = http_client.request_with_retries(
            ParamRequest(endpoint_url="http://nonexistent.domain/api/resource", method="GET")
        )
        response = results[0]
        assert len(results) == 1
        assert isinstance(response, FailedRequestMessage)
        assert "RequestException after 1 connect attempts" in response.error

    def test_read_timeout_error(self, http_client_one_retry: HTTPClient, rsps: responses.RequestsMock) -> None:
        http_client = http_client_one_retry
        rsps.add(
            responses.GET,
            "https://example.com/api/resource",
            body=requests.ReadTimeout("Simulated read timeout"),
        )
        bad_request = ParamRequest(endpoint_url="https://example.com/api/resource", method="GET")
        results = http_client.request_with_retries(bad_request)
        response = results[0]
        assert len(results) == 1
        assert isinstance(response, FailedRequestMessage)
        assert "Simulated read timeout" in response.error
