import json
from collections import Counter
from collections.abc import Iterator
from unittest.mock import patch

import httpx
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses
from cognite_toolkit._cdf_tk.utils.http_client import (
    FailedItem,
    FailedRequestItem,
    FailedRequestMessage,
    FailedResponse,
    HTTPClient,
    HTTPMessage,
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
from tests.test_unit.utils import FakeCogniteResourceGenerator


@pytest.fixture
def rsps() -> Iterator[respx.MockRouter]:
    with respx.mock() as rsps:
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
    def test_get_request(self, rsps: respx.MockRouter, http_client: HTTPClient) -> None:
        rsps.get("https://example.com/api/resource").respond(json={"key": "value"}, status_code=200)
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
    def test_post_request(self, rsps: respx.MockRouter, http_client: HTTPClient) -> None:
        rsps.post("https://example.com/api/resource").respond(json={"id": 123, "status": "created"}, status_code=201)
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
        assert rsps.calls[-1].request.content == json.dumps({"name": "new resource"}).encode()

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

    def test_failed_request(self, rsps: respx.MockRouter, http_client: HTTPClient) -> None:
        rsps.get("https://example.com/api/resource").respond(json={"error": "bad request"}, status_code=400)
        results = http_client.request(
            ParamRequest(endpoint_url="https://example.com/api/resource", method="GET", parameters={"query": "fail"})
        )
        assert len(results) == 1
        response = results[0]
        assert isinstance(response, FailedResponse)
        assert response.status_code == 400
        assert response.error == "bad request"

    @pytest.mark.usefixtures("disable_gzip")
    def test_retry_then_success(self, rsps: respx.MockRouter, http_client: HTTPClient) -> None:
        url = "https://example.com/api/resource"
        rsps.get(url).respond(json={"error": "service unavailable"}, status_code=503)
        rsps.get(url).respond(json={"key": "value"}, status_code=200)
        results = http_client.request_with_retries(ParamRequest(endpoint_url=url, method="GET"))
        assert len(results) == 1
        response = results[0]
        assert isinstance(response, SuccessResponse)
        assert response.status_code == 200
        assert response.body == {"key": "value"}

    def test_retry_exhausted(self, http_client_one_retry: HTTPClient, rsps: respx.MockRouter) -> None:
        client = http_client_one_retry
        for _ in range(2):
            rsps.get("https://example.com/api/resource").respond(
                json={"error": {"message": "service unavailable"}}, status_code=503
            )
        with patch("time.sleep"):  # Patch sleep to speed up the test
            results = client.request_with_retries(
                ParamRequest(endpoint_url="https://example.com/api/resource", method="GET")
            )

        assert len(results) == 1
        response = results[0]
        assert isinstance(response, FailedResponse)
        assert response.status_code == 503
        assert response.error == "service unavailable"

    def test_invalid_json_response(self, rsps: respx.MockRouter, http_client: HTTPClient) -> None:
        rsps.get("https://example.com/api/resource").respond(content="not json", status_code=200)
        results = http_client.request(ParamRequest(endpoint_url="https://example.com/api/resource", method="GET"))
        assert len(results) == 1
        response = results[0]
        assert isinstance(response, FailedResponse)
        assert response.status_code == 200
        assert "Invalid JSON response" in response.error

    def test_connection_error(self, http_client_one_retry: HTTPClient, rsps: respx.MockRouter) -> None:
        http_client = http_client_one_retry
        rsps.get("http://nonexistent.domain/api/resource").mock(
            side_effect=httpx.ConnectError("Simulated connection error")
        )
        results = http_client.request_with_retries(
            ParamRequest(endpoint_url="http://nonexistent.domain/api/resource", method="GET")
        )
        response = results[0]
        assert len(results) == 1
        assert isinstance(response, FailedRequestMessage)
        assert "RequestException after 1 attempts (connect error): Simulated connection error" == response.error

    def test_read_timeout_error(self, http_client_one_retry: HTTPClient, rsps: respx.MockRouter) -> None:
        http_client = http_client_one_retry
        rsps.get("https://example.com/api/resource").mock(side_effect=httpx.ReadTimeout("Simulated read timeout"))
        bad_request = ParamRequest(endpoint_url="https://example.com/api/resource", method="GET")
        results = http_client.request_with_retries(bad_request)
        response = results[0]
        assert len(results) == 1
        assert isinstance(response, FailedRequestMessage)
        assert "RequestException after 1 attempts (read error): Simulated read timeout" == response.error

    def test_zero_retries(self, toolkit_config: ToolkitClientConfig, rsps: respx.MockRouter) -> None:
        client = HTTPClient(toolkit_config, max_retries=0)
        rsps.get("https://example.com/api/resource").respond(json={"error": "service unavailable"}, status_code=503)
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
        with pytest.raises(RuntimeError, match=r"RequestMessage has already been attempted 3 times."):
            http_client.request_with_retries(bad_request)

    def test_error_text(self, http_client: HTTPClient, rsps: respx.MockRouter) -> None:
        rsps.get("https://example.com/api/resource").respond(json={"message": "plain_text"}, status_code=401)
        results = http_client.request(ParamRequest(endpoint_url="https://example.com/api/resource", method="GET"))
        assert len(results) == 1
        response = results[0]
        assert isinstance(response, FailedResponse)
        assert response.status_code == 401
        assert response.error == '{"message":"plain_text"}'

    def test_request_alpha(self, http_client: HTTPClient, rsps: respx.MockRouter) -> None:
        rsps.get("https://example.com/api/alpha/endpoint").respond(json={"key": "value"}, status_code=200)
        results = http_client.request(
            ParamRequest(
                endpoint_url="https://example.com/api/alpha/endpoint",
                method="GET",
                parameters={"query": "test"},
                api_version="alpha",
            )
        )
        assert len(results) == 1
        response = results[0]
        assert isinstance(response, SuccessResponse)
        assert response.status_code == 200
        assert rsps.calls[-1].request.headers["cdf-version"] == "alpha"


@pytest.mark.usefixtures("disable_pypi_check")
class TestHTTPClientItemRequests:
    @pytest.mark.usefixtures("disable_gzip")
    def test_request_with_items_happy_path(self, http_client: HTTPClient, rsps: respx.MockRouter) -> None:
        rsps.post("https://example.com/api/resource").respond(
            json={"items": [{"id": 1, "value": 42}, {"id": 2, "value": 43}]},
            status_code=200,
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
        assert json.loads(rsps.calls[0].request.content) == {
            "items": [{"name": "item1", "id": 1}, {"name": "item2", "id": 2}],
            "autoCreateDirectRelations": True,
        }

    @pytest.mark.usefixtures("disable_gzip")
    def test_request_with_items_issues(self, http_client: HTTPClient, rsps: respx.MockRouter) -> None:
        response_items = [
            {"externalId": "success", "data": 123},
            {"externalId": "unexpected", "data": 999},
        ]

        def server_callback(request: httpx.Request) -> httpx.Response:
            # Check request body content
            body_content = request.content.decode() if request.content else ""
            if "fail" in body_content:
                return httpx.Response(400, json={"error": "Item failed"})
            elif "success" in body_content:
                return httpx.Response(200, json={"items": response_items})
            else:
                return httpx.Response(200, json={"items": []})

        rsps.post("https://example.com/api/resource").mock(side_effect=server_callback)

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
        assert json.loads(first.request.content)["items"] == [
            {"externalId": "success"},
            {"externalId": "missing"},
            {"externalId": "fail"},
        ]
        # Second succeeds with 1 item.
        assert json.loads(second.request.content)["items"] == [{"externalId": "success"}]
        # Third fails with two items, and splits into 1 + 1
        assert json.loads(third.request.content)["items"] == [{"externalId": "missing"}, {"externalId": "fail"}]
        # Fourth succeeds with 1 item.
        assert json.loads(fourth.request.content)["items"] == [{"externalId": "missing"}]
        # Fifth fails with 1 item.
        assert json.loads(fifth.request.content)["items"] == [{"externalId": "fail"}]

    def test_request_all_item_fail(self, http_client: HTTPClient, rsps: respx.MockRouter) -> None:
        rsps.post("https://example.com/api/resource").respond(
            json={"error": "Unauthorized"},
            status_code=401,
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

    def test_bad_request_items(self, http_client: HTTPClient, rsps: respx.MockRouter) -> None:
        # Test with non-serializable item
        bad_items = [
            {"id": 1},
            {"externalId": "duplicate"},
            {"externalId": "duplicate"},
        ]  # Duplicate externalId will cause issue
        rsps.post("https://example.com/api/resource").respond(
            json={"items": [{"externalId": "duplicate", "data": 123}]},
            status_code=200,
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

    def test_request_no_items_response(self, http_client: HTTPClient, rsps: respx.MockRouter) -> None:
        rsps.post("https://example.com/api/resource/delete").respond(status_code=200)
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

    def test_response_unknown_id(self, http_client: HTTPClient, rsps: respx.MockRouter) -> None:
        rsps.post("https://example.com/api/resource").respond(
            json={"items": [{"uid": "a", "data": 1}, {"uid": "b", "data": 2}]},
            status_code=200,
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

    def test_timeout_error(self, http_client_one_retry: HTTPClient, rsps: respx.MockRouter) -> None:
        client = http_client_one_retry
        rsps.post("https://example.com/api/resource").mock(side_effect=httpx.ReadTimeout("Simulated timeout error"))
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

    @pytest.mark.usefixtures("disable_gzip")
    def test_early_failure(self, http_client_one_retry: HTTPClient, rsps: respx.MockRouter) -> None:
        client = http_client_one_retry
        rsps.post("https://example.com/api/resource").respond(
            json={"error": "Server error"},
            status_code=400,
        )
        with patch("time.sleep"):
            results = client.request_with_retries(
                ItemsRequest[int](
                    endpoint_url="https://example.com/api/resource",
                    method="POST",
                    items=[{"id": i} for i in range(1000)],
                    as_id=lambda item: item["id"],
                    max_failures_before_abort=5,
                )
            )
        assert len(rsps.calls) == 5
        actual_items_per_request = [len(json.loads(call.request.content)["items"]) for call in rsps.calls]
        assert actual_items_per_request == [1000, 500, 500, 250, 250]  # Splits in half each time
        failures = Counter([type(results) for results in results])
        assert failures == {
            FailedItem: 250,  # 250 items keeps the original error message.
            FailedRequestItem: 750,  # 750 items get the early abort message.
        }

    @pytest.mark.usefixtures("disable_gzip")
    def test_abort_on_first_failure(self, http_client: HTTPClient, rsps: respx.MockRouter) -> None:
        client = http_client
        rsps.post("https://example.com/api/resource").respond(
            json={"error": "Server error"},
            status_code=400,
        )
        results = client.request_with_retries(
            ItemsRequest[int](
                endpoint_url="https://example.com/api/resource",
                method="POST",
                items=[{"id": i} for i in range(1000)],
                as_id=lambda item: item["id"],
                max_failures_before_abort=1,
            )
        )
        actual_failure_types = Counter([type(results) for results in results])
        assert actual_failure_types == {FailedItem: 1000}
        assert len(rsps.calls) == 1
        actual_items_per_request = [len(json.loads(call.request.content)["items"]) for call in rsps.calls]
        assert actual_items_per_request == [1000]

    @pytest.mark.usefixtures("disable_gzip")
    def test_abort_on_second_failure(self, http_client: HTTPClient, rsps: respx.MockRouter) -> None:
        client = http_client
        rsps.post("https://example.com/api/resource").respond(
            json={"error": "Server error"},
            status_code=400,
        )
        results = client.request_with_retries(
            ItemsRequest[int](
                endpoint_url="https://example.com/api/resource",
                method="POST",
                items=[{"id": i} for i in range(1000)],
                as_id=lambda item: item["id"],
                max_failures_before_abort=2,
            )
        )
        actual_failure_types = Counter([type(results) for results in results])
        assert actual_failure_types == {FailedItem: 500, FailedRequestItem: 500}
        assert len(rsps.calls) == 2
        actual_items_per_request = [len(json.loads(call.request.content)["items"]) for call in rsps.calls]
        assert actual_items_per_request == [1000, 500]

    @pytest.mark.usefixtures("disable_gzip")
    def test_never_abort_on_failure(self, http_client: HTTPClient, rsps: respx.MockRouter) -> None:
        client = http_client
        rsps.post("https://example.com/api/resource").respond(
            json={"error": "Server error"},
            status_code=400,
        )
        results = client.request_with_retries(
            ItemsRequest[int](
                endpoint_url="https://example.com/api/resource",
                method="POST",
                items=[{"id": i} for i in range(100)],
                as_id=lambda item: item["id"],
                max_failures_before_abort=-1,  # Never abort
            )
        )
        actual_failure_types = Counter([type(results) for results in results])
        assert actual_failure_types == {FailedItem: 100}
        assert len(rsps.calls) == 199

    @pytest.mark.usefixtures("disable_gzip")
    def test_failing_3_items(self, http_client_one_retry: HTTPClient, rsps: respx.MockRouter) -> None:
        client = http_client_one_retry

        def dislike_942_112_and_547(request: httpx.Request) -> httpx.Response:
            # Check request body content
            body_content = request.content.decode() if request.content else ""
            for no in ["942", "112", "547"]:
                if no in body_content:
                    return httpx.Response(400, json={"error": f"Item {no} is not allowed"})

            # Parse the request body to create response items
            try:
                body_data = json.loads(body_content)
                items = body_data.get("items", [])
                response_items = [{"id": item["id"], "status": "ok"} for item in items]
                return httpx.Response(200, json={"items": response_items})
            except (json.JSONDecodeError, KeyError):
                return httpx.Response(200, json={"items": []})

        rsps.post("https://example.com/api/resource").mock(side_effect=dislike_942_112_and_547)
        with patch("time.sleep"):
            results = client.request_with_retries(
                ItemsRequest[int](
                    endpoint_url="https://example.com/api/resource",
                    method="POST",
                    items=[{"id": i} for i in range(1000)],
                    as_id=lambda item: item["id"],
                    max_failures_before_abort=30,
                )
            )
        failures = Counter([type(results) for results in results])
        assert failures == {FailedItem: 3, SuccessItem: 997}


class TestHTTPMessage:
    @pytest.mark.parametrize("message_cls", get_concrete_subclasses(HTTPMessage))
    def test_dump_http_message(self, message_cls: type[HTTPMessage]) -> None:
        message = FakeCogniteResourceGenerator(seed=42).create_instance(message_cls)

        dumped = message.dump()
        assert isinstance(dumped, dict)
        assert dumped["type"] == message_cls.__name__
        try:
            json.dumps(dumped)
        except (TypeError, ValueError) as e:
            pytest.fail(f"Dumped data is not valid JSON: {e}")
