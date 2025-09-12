import json
from collections.abc import Iterator
from unittest.mock import patch

import pytest
import requests
import responses

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.utils.http_client import (
    FailedItem,
    FailedRequestItem,
    FailedRequestMessage,
    FailedResponse,
    HTTPClient,
    ItemsRequest,
    MissingItem,
    ParamRequest,
    SimpleBodyRequest,
    SuccessItem,
    SuccessResponse,
    UnexpectedItem,
    UnknownRequestItem,
    UnknownResponseItem,
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
        assert "RequestException after 1 attempts (connect error): Simulated connection error" == response.error

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
        assert "RequestException after 1 attempts (read error): Simulated read timeout" == response.error

    def test_zero_retries(self, toolkit_config: ToolkitClientConfig, rsps: responses.RequestsMock) -> None:
        client = HTTPClient(toolkit_config, max_retries=0)
        rsps.get("https://example.com/api/resource", json={"error": "service unavailable"}, status=503)
        results = client.request_with_retries(
            ParamRequest(endpoint_url="https://example.com/api/resource", method="GET")
        )
        assert len(results) == 1
        response = results[0]
        assert isinstance(response, FailedResponse)
        assert response.status_code == 503
        assert response.error == "service unavailable"
        assert len(rsps.calls) == 1

    def test_raise_if_already_retied(self, http_client_one_retry: HTTPClient) -> None:
        http_client = http_client_one_retry
        bad_request = ParamRequest(endpoint_url="https://example.com/api/resource", method="GET", status_attempt=3)
        with pytest.raises(RuntimeError, match="RequestMessage has already been attempted 3 times."):
            http_client.request_with_retries(bad_request)

    def test_error_text(self, http_client: HTTPClient, rsps: responses.RequestsMock) -> None:
        rsps.get("https://example.com/api/resource", json={"message": "plain_text"}, status=401)
        results = http_client.request(ParamRequest(endpoint_url="https://example.com/api/resource", method="GET"))
        assert len(results) == 1
        response = results[0]
        assert isinstance(response, FailedResponse)
        assert response.status_code == 401
        assert response.error == '{"message": "plain_text"}'


@pytest.mark.usefixtures("disable_pypi_check")
class TestHTTPClientItemRequests:
    @pytest.mark.usefixtures("disable_gzip")
    def test_request_with_items_happy_path(self, http_client: HTTPClient, rsps: responses.RequestsMock) -> None:
        rsps.post(
            "https://example.com/api/resource",
            json={"items": [{"id": 1, "value": 42}, {"id": 2, "value": 43}]},
            status=200,
        )
        items = [{"name": "item1", "id": 1}, {"name": "item2", "id": 2}]
        results = http_client.request(
            ItemsRequest[int](
                endpoint_url="https://example.com/api/resource",
                method="POST",
                items=items,
                extra_body_fields={"autoCreateDirectRelations": True},
                as_id=lambda item: item["id"],
            )
        )
        assert results == [
            SuccessItem(status_code=200, id=1, item={"id": 1, "value": 42}),
            SuccessItem(status_code=200, id=2, item={"id": 2, "value": 43}),
        ]
        assert len(rsps.calls) == 1
        assert json.loads(rsps.calls[0].request.body) == {
            "items": [{"name": "item1", "id": 1}, {"name": "item2", "id": 2}],
            "autoCreateDirectRelations": True,
        }

    @pytest.mark.usefixtures("disable_gzip")
    def test_request_with_items_issues(self, http_client: HTTPClient, rsps: responses.RequestsMock) -> None:
        response_items = [
            {"externalId": "success", "data": 123},
            {"externalId": "unexpected", "data": 999},
        ]

        def server_callback(request: requests.PreparedRequest) -> tuple[int, dict[str, str], str]:
            # status, headers, body
            if "fail" in request.body:
                return 400, {}, json.dumps({"error": "Item failed"})
            elif "success" in request.body:
                return 200, {}, json.dumps({"items": response_items})
            else:
                return 200, {}, json.dumps({"items": []})

        rsps.add_callback(
            method=responses.POST,
            url="https://example.com/api/resource",
            callback=server_callback,
            content_type="application/json",
        )

        request_items = [
            {"externalId": "success"},
            {"externalId": "missing"},
            {"externalId": "fail"},
        ]
        results = http_client.request_with_retries(
            ItemsRequest[str](
                endpoint_url="https://example.com/api/resource",
                method="POST",
                items=request_items,
                as_id=lambda item: item["externalId"],
            )
        )
        assert results == [
            SuccessItem(status_code=200, id="success", item={"externalId": "success", "data": 123}),
            UnexpectedItem(status_code=200, id="unexpected", item={"externalId": "unexpected", "data": 999}),
            MissingItem(status_code=200, id="missing"),
            FailedItem(status_code=400, id="fail", error="Item failed"),
        ]
        assert len(rsps.calls) == 5  # Three requests made
        first, second, third, fourth, fifth = rsps.calls
        # First call will fail, and split into 1 item + 2 items
        assert json.loads(first.request.body)["items"] == [
            {"externalId": "success"},
            {"externalId": "missing"},
            {"externalId": "fail"},
        ]
        # Second succeeds with 1 item.
        assert json.loads(second.request.body)["items"] == [{"externalId": "success"}]
        # Third fails with two items, and splits into 1 + 1
        assert json.loads(third.request.body)["items"] == [{"externalId": "missing"}, {"externalId": "fail"}]
        # Fourth succeeds with 1 item.
        assert json.loads(fourth.request.body)["items"] == [{"externalId": "missing"}]
        # Fifth fails with 1 item.
        assert json.loads(fifth.request.body)["items"] == [{"externalId": "fail"}]

    def test_request_all_item_fail(self, http_client: HTTPClient, rsps: responses.RequestsMock) -> None:
        rsps.post(
            "https://example.com/api/resource",
            json={"error": "Unauthorized"},
            status=401,
        )
        items = [{"name": "item1", "id": 1}, {"name": "item2", "id": 2}]
        results = http_client.request(
            ItemsRequest[int](
                endpoint_url="https://example.com/api/resource",
                method="POST",
                items=items,
                as_id=lambda item: item["id"],
            )
        )
        assert results == [
            FailedItem(status_code=401, id=1, error="Unauthorized"),
            FailedItem(status_code=401, id=2, error="Unauthorized"),
        ]

        assert len(rsps.calls) == 1  # Only one request made

    def test_bad_request_items(self, http_client: HTTPClient, rsps: responses.RequestsMock) -> None:
        # Test with non-serializable item
        bad_items = [
            {"id": 1},
            {"externalId": "duplicate"},
            {"externalId": "duplicate"},
        ]  # Duplicate externalId will cause issue
        rsps.post(
            "https://example.com/api/resource",
            json={"items": [{"externalId": "duplicate", "data": 123}]},
            status=200,
        )

        results = http_client.request(
            ItemsRequest[str](
                endpoint_url="https://example.com/api/resource",
                method="POST",
                items=bad_items,
                as_id=lambda item: item["externalId"],  # KeyError will occur here
            )
        )
        assert results == [
            UnknownRequestItem(error="Error extracting ID: 'externalId'", item={"id": 1}),
            FailedRequestItem(id="duplicate", error="Duplicate item ID: 'duplicate'"),
            SuccessItem(status_code=200, id="duplicate", item={"externalId": "duplicate", "data": 123}),
        ]

    def test_request_no_items_response(self, http_client: HTTPClient, rsps: responses.RequestsMock) -> None:
        rsps.post(
            "https://example.com/api/resource/delete",
            status=200,
        )
        items = [{"id": 1}, {"id": 2}]
        results = http_client.request(
            ItemsRequest[int](
                endpoint_url="https://example.com/api/resource/delete",
                method="POST",
                items=items,
                as_id=lambda item: item["id"],
            )
        )
        assert results == [
            SuccessItem(status_code=200, id=1),
            SuccessItem(status_code=200, id=2),
        ]

    def test_response_unknown_id(self, http_client: HTTPClient, rsps: responses.RequestsMock) -> None:
        rsps.post(
            "https://example.com/api/resource",
            json={"items": [{"uid": "a", "data": 1}, {"uid": "b", "data": 2}]},
            status=200,
        )
        items = [{"name": "itemA", "id": 1}, {"name": "itemB", "id": 2}]
        results = http_client.request(
            ItemsRequest[int](
                endpoint_url="https://example.com/api/resource",
                method="POST",
                items=items,
                as_id=lambda item: item["id"],
            )
        )
        assert results == [
            UnknownResponseItem(status_code=200, item={"uid": "a", "data": 1}, error="Error extracting ID: 'id'"),
            UnknownResponseItem(status_code=200, item={"uid": "b", "data": 2}, error="Error extracting ID: 'id'"),
            MissingItem(status_code=200, id=1),
            MissingItem(status_code=200, id=2),
        ]

    def test_timeout_error(self, http_client_one_retry: HTTPClient, rsps: responses.RequestsMock) -> None:
        client = http_client_one_retry
        rsps.add(
            responses.POST,
            "https://example.com/api/resource",
            body=requests.ReadTimeout("Simulated timeout error"),
        )
        with patch("time.sleep"):
            results = client.request_with_retries(
                ItemsRequest[int](
                    endpoint_url="https://example.com/api/resource",
                    method="POST",
                    items=[{"id": 1}],
                    as_id=lambda item: item["id"],
                )
            )
        assert results == [
            FailedRequestItem(id=1, error="RequestException after 1 attempts (read error): Simulated timeout error")
        ]
