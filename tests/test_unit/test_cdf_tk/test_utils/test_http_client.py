import json
from collections import Counter
from collections.abc import Iterator
from unittest.mock import patch

import httpx
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client._resource_base import RequestItem
from cognite_toolkit._cdf_tk.client.http_client import (
    ErrorDetails2,
    FailedRequest2,
    FailedResponse2,
    HTTPClient,
    ItemsFailedRequest2,
    ItemsFailedResponse2,
    ItemsRequest2,
    ItemsSuccessResponse2,
    RequestMessage2,
    SuccessResponse2,
)


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


@pytest.mark.usefixtures("disable_pypi_check")
class TestHTTPClient2:
    def test_get_request(self, rsps: respx.MockRouter, http_client: HTTPClient) -> None:
        rsps.get("https://example.com/api/resource").respond(json={"key": "value"}, status_code=200)
        response = http_client.request_single(
            RequestMessage2(endpoint_url="https://example.com/api/resource", method="GET", parameters={"query": "test"})
        )
        assert isinstance(response, SuccessResponse2)
        assert response.status_code == 200
        assert response.body == '{"key":"value"}'
        assert rsps.calls[-1].request.url == "https://example.com/api/resource?query=test"

    @pytest.mark.usefixtures("disable_gzip")
    def test_post_request(self, rsps: respx.MockRouter, http_client: HTTPClient) -> None:
        rsps.post("https://example.com/api/resource").respond(json={"id": 123, "status": "created"}, status_code=201)
        response = http_client.request_single(
            RequestMessage2(
                endpoint_url="https://example.com/api/resource",
                method="POST",
                body_content={"values": [float("nan")], "other": float("inf")},
            )
        )
        assert isinstance(response, SuccessResponse2)
        assert response.status_code == 201
        assert response.body == '{"id":123,"status":"created"}'
        assert rsps.calls[-1].request.content == b'{"values":[null],"other":null}'

    def test_failed_request(self, rsps: respx.MockRouter, http_client: HTTPClient) -> None:
        rsps.get("https://example.com/api/resource").respond(
            json={"error": {"message": "bad request", "code": 400}}, status_code=400
        )
        response = http_client.request_single(
            RequestMessage2(endpoint_url="https://example.com/api/resource", method="GET", parameters={"query": "fail"})
        )
        assert isinstance(response, FailedResponse2)
        assert response.status_code == 400
        assert response.error.message == "bad request"

    @pytest.mark.usefixtures("disable_gzip")
    def test_retry_then_success(self, rsps: respx.MockRouter, http_client: HTTPClient) -> None:
        url = "https://example.com/api/resource"
        rsps.get(url).respond(json={"error": "service unavailable"}, status_code=503)
        rsps.get(url).respond(json={"key": "value"}, status_code=200)
        response = http_client.request_single_retries(RequestMessage2(endpoint_url=url, method="GET"))
        assert isinstance(response, SuccessResponse2)
        assert response.status_code == 200
        assert response.body == '{"key":"value"}'

    def test_retry_exhausted(self, http_client_one_retry: HTTPClient, rsps: respx.MockRouter) -> None:
        client = http_client_one_retry
        for _ in range(2):
            rsps.get("https://example.com/api/resource").respond(
                json={"error": {"message": "service unavailable", "code": 503}}, status_code=503
            )
        with patch("time.sleep"):  # Patch sleep to speed up the test
            response = client.request_single_retries(
                RequestMessage2(endpoint_url="https://example.com/api/resource", method="GET")
            )

        assert isinstance(response, FailedResponse2)
        assert response.status_code == 503
        assert response.error.message == "service unavailable"

    def test_connection_error(self, http_client_one_retry: HTTPClient, rsps: respx.MockRouter) -> None:
        http_client = http_client_one_retry
        rsps.get("http://nonexistent.domain/api/resource").mock(
            side_effect=httpx.ConnectError("Simulated connection error")
        )
        with patch(f"{HTTPClient.__module__}.time"):
            # Patch time to avoid actual sleep
            response = http_client.request_single_retries(
                RequestMessage2(endpoint_url="http://nonexistent.domain/api/resource", method="GET")
            )
        assert isinstance(response, FailedRequest2)
        assert "RequestException after 1 attempts (connect error): Simulated connection error" == response.error

    def test_read_timeout_error(self, http_client_one_retry: HTTPClient, rsps: respx.MockRouter) -> None:
        http_client = http_client_one_retry
        rsps.get("https://example.com/api/resource").mock(side_effect=httpx.ReadTimeout("Simulated read timeout"))
        with patch(f"{HTTPClient.__module__}.time"):
            # Patch time to avoid actual sleep
            response = http_client.request_single_retries(
                RequestMessage2(endpoint_url="https://example.com/api/resource", method="GET")
            )
        assert isinstance(response, FailedRequest2)
        assert "RequestException after 1 attempts (read error): Simulated read timeout" == response.error

    def test_zero_retries(self, toolkit_config: ToolkitClientConfig, rsps: respx.MockRouter) -> None:
        client = HTTPClient(toolkit_config, max_retries=0)
        rsps.get("https://example.com/api/resource").respond(
            json={"error": {"message": "service unavailable", "code": 503}}, status_code=503
        )
        with patch("time.sleep"):  # Patch sleep to speed up the test
            response = client.request_single_retries(
                RequestMessage2(endpoint_url="https://example.com/api/resource", method="GET")
            )
        assert isinstance(response, FailedResponse2)
        assert response.status_code == 503
        assert response.error.message == "service unavailable"
        assert len(rsps.calls) == 1

    def test_raise_if_already_retied(self, http_client_one_retry: HTTPClient) -> None:
        http_client = http_client_one_retry
        bad_request = RequestMessage2(endpoint_url="https://example.com/api/resource", method="GET", status_attempt=3)
        with pytest.raises(RuntimeError, match=r"RequestMessage has already been attempted 3 times."):
            http_client.request_single_retries(bad_request)

    def test_error_text(self, http_client: HTTPClient, rsps: respx.MockRouter) -> None:
        rsps.get("https://example.com/api/resource").respond(json={"message": "plain_text"}, status_code=401)
        response = http_client.request_single(
            RequestMessage2(endpoint_url="https://example.com/api/resource", method="GET")
        )
        assert isinstance(response, FailedResponse2)
        assert response.status_code == 401
        assert response.error.message == '{"message":"plain_text"}'

    def test_request_alpha(self, http_client: HTTPClient, rsps: respx.MockRouter) -> None:
        rsps.get("https://example.com/api/alpha/endpoint").respond(json={"key": "value"}, status_code=200)
        response = http_client.request_single(
            RequestMessage2(
                endpoint_url="https://example.com/api/alpha/endpoint",
                method="GET",
                parameters={"query": "test"},
                api_version="alpha",
            )
        )
        assert isinstance(response, SuccessResponse2)
        assert response.status_code == 200
        assert rsps.calls[-1].request.headers["cdf-version"] == "alpha"


class MyRequestItem(RequestItem):
    name: str
    id: int

    def __str__(self) -> str:
        return str(self.id)


@pytest.mark.usefixtures("disable_pypi_check")
class TestHTTPClientItemRequests2:
    @pytest.mark.usefixtures("disable_gzip")
    def test_request_with_items_happy_path(self, http_client: HTTPClient, rsps: respx.MockRouter) -> None:
        rsps.post("https://example.com/api/resource").respond(
            json={"items": [{"id": 1, "value": 42}, {"id": 2, "value": 43}]},
            status_code=200,
        )
        items = [MyRequestItem(name="A", id=1), MyRequestItem(name="B", id=2)]
        results = http_client.request_items(
            ItemsRequest2(
                endpoint_url="https://example.com/api/resource",
                method="POST",
                items=items,
                extra_body_fields={"autoCreateDirectRelations": True},
            )
        )
        body = '{"items":[{"id":1,"value":42},{"id":2,"value":43}]}'
        assert results == [
            ItemsSuccessResponse2(status_code=200, ids=["1", "2"], body=body, content=body.encode("utf-8"))
        ]
        assert len(rsps.calls) == 1
        assert json.loads(rsps.calls[0].request.content) == {
            "items": [{"name": "A", "id": 1}, {"name": "B", "id": 2}],
            "autoCreateDirectRelations": True,
        }

    @pytest.mark.usefixtures("disable_gzip")
    def test_request_with_items_issues(self, http_client: HTTPClient, rsps: respx.MockRouter) -> None:
        response_items = [
            {"externalId": "success", "data": 123},
        ]

        def server_callback(request: httpx.Request) -> httpx.Response:
            # Check request body content
            body_content = request.content.decode() if request.content else ""
            if "fail" in body_content:
                return httpx.Response(400, json={"error": {"message": "Item failed", "code": 400}})
            elif "success" in body_content:
                return httpx.Response(200, json={"items": response_items})
            else:
                return httpx.Response(200, json={"items": []})

        rsps.post("https://example.com/api/resource").mock(side_effect=server_callback)

        request_items = [
            MyRequestItem(name="success", id=1),
            MyRequestItem(name="fail", id=2),
        ]
        results = http_client.request_items_retries(
            ItemsRequest2(
                endpoint_url="https://example.com/api/resource",
                method="POST",
                items=request_items,
            )
        )
        body = '{"items":[{"externalId":"success","data":123}]}'
        assert results == [
            ItemsSuccessResponse2(status_code=200, ids=["1"], body=body, content=body.encode("utf-8")),
            ItemsFailedResponse2(
                status_code=400,
                ids=["2"],
                error=ErrorDetails2(message="Item failed", code=400),
                body='{"error":{"message":"Item failed","code":400}}',
            ),
        ]
        assert len(rsps.calls) == 3  # Three requests made
        first, second, third = rsps.calls
        # First call will fail, and split into 1 item + 2 items
        assert json.loads(first.request.content)["items"] == [
            {"name": "success", "id": 1},
            {"name": "fail", "id": 2},
        ]
        # Second succeeds with 1 item.
        assert json.loads(second.request.content)["items"] == [{"name": "success", "id": 1}]
        # Third fails with 1 item.
        assert json.loads(third.request.content)["items"] == [{"name": "fail", "id": 2}]

    def test_request_all_item_fail(self, http_client: HTTPClient, rsps: respx.MockRouter) -> None:
        rsps.post("https://example.com/api/resource").respond(
            json={"error": {"message": "Unauthorized", "code": 401}},
            status_code=401,
        )
        items = [MyRequestItem(name="item1", id=1), MyRequestItem(name="item2", id=2)]
        results = http_client.request_items(
            ItemsRequest2(
                endpoint_url="https://example.com/api/resource",
                method="POST",
                items=items,
            )
        )
        assert results == [
            ItemsFailedResponse2(
                status_code=401,
                ids=["1", "2"],
                error=ErrorDetails2(message="Unauthorized", code=401),
                body='{"error":{"message":"Unauthorized","code":401}}',
            ),
        ]
        assert len(rsps.calls) == 1

    def test_request_no_items_response(self, http_client: HTTPClient, rsps: respx.MockRouter) -> None:
        rsps.post("https://example.com/api/resource/delete").respond(status_code=200)
        items = [MyRequestItem(name="A", id=1), MyRequestItem(name="B", id=2)]
        results = http_client.request_items(
            ItemsRequest2(
                endpoint_url="https://example.com/api/resource/delete",
                method="POST",
                items=items,
            )
        )
        assert results == [
            ItemsSuccessResponse2(status_code=200, ids=["1", "2"], body="", content=b""),
        ]

    def test_timeout_error(self, http_client_one_retry: HTTPClient, rsps: respx.MockRouter) -> None:
        client = http_client_one_retry
        rsps.post("https://example.com/api/resource").mock(side_effect=httpx.ReadTimeout("Simulated timeout error"))
        with patch("time.sleep"):
            results = client.request_items_retries(
                ItemsRequest2(
                    endpoint_url="https://example.com/api/resource",
                    method="POST",
                    items=[MyRequestItem(name="A", id=1)],
                )
            )
        assert results == [
            ItemsFailedRequest2(
                ids=["1"], error_message="RequestException after 1 attempts (read error): Simulated timeout error"
            )
        ]

    @pytest.mark.usefixtures("disable_gzip")
    def test_early_failure(self, http_client_one_retry: HTTPClient, rsps: respx.MockRouter) -> None:
        client = http_client_one_retry
        rsps.post("https://example.com/api/resource").respond(
            json={"error": {"message": "Server error", "code": 400}},
            status_code=400,
        )
        with patch("time.sleep"):
            results = client.request_items_retries(
                ItemsRequest2(
                    endpoint_url="https://example.com/api/resource",
                    method="POST",
                    items=[MyRequestItem(name=str(i), id=i) for i in range(1000)],
                    max_failures_before_abort=5,
                )
            )
        assert len(rsps.calls) == 5
        actual_items_per_request = [len(json.loads(call.request.content)["items"]) for call in rsps.calls]
        assert actual_items_per_request == [1000, 500, 500, 250, 250]
        failures = Counter([type(result) for result in results for _ in getattr(result, "ids", [])])
        assert failures == {
            ItemsFailedResponse2: 250,
            ItemsFailedRequest2: 750,
        }

    @pytest.mark.usefixtures("disable_gzip")
    def test_abort_on_first_failure(self, http_client: HTTPClient, rsps: respx.MockRouter) -> None:
        client = http_client
        rsps.post("https://example.com/api/resource").respond(
            json={"error": {"message": "Server error", "code": 400}},
            status_code=400,
        )
        results = client.request_items_retries(
            ItemsRequest2(
                endpoint_url="https://example.com/api/resource",
                method="POST",
                items=[MyRequestItem(name=str(i), id=i) for i in range(1000)],
                max_failures_before_abort=1,
            )
        )
        actual_failure_types = Counter([type(results) for results in results])
        assert actual_failure_types == {ItemsFailedResponse2: 1}
        assert len(rsps.calls) == 1
        actual_items_per_request = [len(json.loads(call.request.content)["items"]) for call in rsps.calls]
        assert actual_items_per_request == [1000]

    @pytest.mark.usefixtures("disable_gzip")
    def test_abort_on_second_failure(self, http_client: HTTPClient, rsps: respx.MockRouter) -> None:
        client = http_client
        rsps.post("https://example.com/api/resource").respond(
            json={"error": {"message": "Server error", "code": 400}},
            status_code=400,
        )
        results = client.request_items_retries(
            ItemsRequest2(
                endpoint_url="https://example.com/api/resource",
                method="POST",
                items=[MyRequestItem(name=str(i), id=i) for i in range(1000)],
                max_failures_before_abort=2,
            )
        )
        actual_failure_types = Counter([type(results) for results in results for _ in getattr(results, "ids", [])])
        assert actual_failure_types == {ItemsFailedResponse2: 500, ItemsFailedRequest2: 500}
        assert len(rsps.calls) == 2
        actual_items_per_request = [len(json.loads(call.request.content)["items"]) for call in rsps.calls]
        assert actual_items_per_request == [1000, 500]

    @pytest.mark.usefixtures("disable_gzip")
    def test_never_abort_on_failure(self, http_client: HTTPClient, rsps: respx.MockRouter) -> None:
        client = http_client
        rsps.post("https://example.com/api/resource").respond(
            json={"error": {"message": "Server error", "code": 400}},
            status_code=400,
        )
        results = client.request_items_retries(
            ItemsRequest2(
                endpoint_url="https://example.com/api/resource",
                method="POST",
                items=[MyRequestItem(name=str(i), id=i) for i in range(100)],
                max_failures_before_abort=-1,
            )
        )
        actual_failure_types = Counter([type(results) for results in results for _ in getattr(results, "ids", [])])
        assert actual_failure_types == {ItemsFailedResponse2: 100}
        assert len(rsps.calls) == 199

    @pytest.mark.usefixtures("disable_gzip")
    def test_failing_3_items(self, http_client_one_retry: HTTPClient, rsps: respx.MockRouter) -> None:
        client = http_client_one_retry

        def dislike_942_112_and_547(request: httpx.Request) -> httpx.Response:
            # Check request body content
            body_content = request.content.decode() if request.content else ""
            for no in ["942", "112", "547"]:
                if no in body_content:
                    return httpx.Response(400, json={"error": {"message": f"Item {no} is not allowed", "code": 400}})

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
            results = client.request_items_retries(
                ItemsRequest2(
                    endpoint_url="https://example.com/api/resource",
                    method="POST",
                    items=[MyRequestItem(name=str(i), id=i) for i in range(1000)],
                    max_failures_before_abort=30,
                )
            )
        failures = Counter([type(results) for results in results for _ in getattr(results, "ids", [])])
        assert failures == {ItemsFailedResponse2: 3, ItemsSuccessResponse2: 997}


class TestItemMessage:
    def test_tracker_correctly_set(self) -> None:
        message = ItemsRequest2(
            endpoint_url="https://example.com/api/resource",
            method="POST",
            items=[MyRequestItem(name="A", id=1)],
            max_failures_before_abort=10,
        )

        assert message.tracker.max_failures_before_abort == 10
