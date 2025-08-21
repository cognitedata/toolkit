import threading
import time
from collections.abc import Iterator
from unittest.mock import MagicMock, patch

import pytest
import requests
import responses
from cognite.client import global_config
from cognite.client.credentials import OAuthClientCredentials

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.utils.auth import CLIENT_NAME
from cognite_toolkit._cdf_tk.utils.batch_processor import (
    BatchResult,
    HTTPBatchProcessor,
    HTTPIterableProcessor,
    SuccessItem,
)


@pytest.fixture
def toolkit_config() -> ToolkitClientConfig:
    global_config.disable_pypi_version_check = True
    credentials = MagicMock(spec=OAuthClientCredentials)
    credentials.client_id = "toolkit-client-id"
    credentials.client_secret = "toolkit-client-secret"
    credentials.token_url = "https://toolkit.auth.com/oauth/token"
    credentials.scopes = ["ttps://pytest-field.cognitedata.com/.default"]
    credentials.authorization_header.return_value = "Auth", "Bearer dummy"
    return ToolkitClientConfig(
        client_name=CLIENT_NAME,
        project="pytest-project",
        credentials=credentials,
        is_strict_validation=False,
    )


@pytest.fixture
def processor(toolkit_config: ToolkitClientConfig) -> Iterator[HTTPIterableProcessor]:
    with HTTPIterableProcessor(
        endpoint_url="https://test.com/api",
        config=toolkit_config,
        as_id=lambda item: item["id"],
        max_workers=4,
        batch_size=100,
    ) as processor:
        yield processor


class TestHTTPIterableProcessor:
    def test_happy_path(self, toolkit_config: ToolkitClientConfig) -> None:
        url = "http://example.com/api"
        processor: HTTPIterableProcessor[str]
        with (
            HTTPIterableProcessor[str](
                url,
                toolkit_config,
                lambda item: item["externalId"],
            ) as processor,
            responses.RequestsMock() as rsps,
        ):
            rsps.add(
                responses.POST,
                url,
                status=200,
                json={
                    "items": [
                        {"externalId": "item1", "server": "property1"},
                        {"externalId": "item2", "server": "property2"},
                    ]
                },
            )
            result = processor.process(
                (
                    {
                        "externalId": "item1",
                    },
                    {"externalId": "item2"},
                )
            )

            assert len(rsps.calls) == 1
            assert result.total_successful == 2
            assert result.total_processed == 2
            assert result.success_rate == 1.0
            assert [item.response for item in result.successful_items if item.response] == [
                {"externalId": "item1", "server": "property1"},
                {"externalId": "item2", "server": "property2"},
            ]

    def test_concurrent_processing(self, processor: HTTPIterableProcessor) -> None:
        """Test that items are processed concurrently using multiple workers"""
        with responses.RequestsMock() as rsps:
            # Add a callback to simulate delay and track request times
            request_times = []

            def request_callback(request):
                request_times.append(time.time())
                time.sleep(0.1)
                return 200, {}, '{"items": []}'

            rsps.add_callback(responses.POST, processor.endpoint_url, callback=request_callback)

            items = [{"id": i, "name": f"Item {i}"} for i in range(400)]

            start_time = time.time()
            result = processor.process(items, total_items=len(items))
            end_time = time.time()

            assert len(request_times) == processor.max_workers
            # With 4 workers, we should have at least some parallel requests
            # Check that requests were made in parallel by verifying multiple requests
            # were initiated before the first one completed
            first_request_complete = request_times[0] + 0.1  # 0.1s delay per request
            parallel_requests = sum(1 for t in request_times if t < first_request_complete)
            assert parallel_requests > 1

            # Verify the total time is less than if it were sequential
            assert end_time - start_time < processor.max_workers * 0.1
            assert result.total_processed == 400

    @pytest.mark.usefixtures("disable_gzip")
    def test_split_batch_concurrency(self, processor: HTTPIterableProcessor) -> None:
        """Test batch splitting under concurrent load"""
        processed_batch_counts = []

        def request_callback(request):
            # Extract the batch size from the request payload
            batch_count = request.body.count('"id":')
            processed_batch_counts.append(batch_count)

            # First batch (size 100) gets an error, others succeed
            if batch_count == 100:
                return 502, {}, '{"error": {"message": "Server Error"}}'
            else:
                return 200, {}, request.body

        with responses.RequestsMock() as rsps:
            rsps.add_callback(
                responses.POST,
                processor.endpoint_url,
                callback=request_callback,
            )

            items = [{"id": i} for i in range(100)]
            result = processor.process(items, total_items=len(items))

            assert processed_batch_counts == [100, 50, 50], "Batch should be split into 100, 50, 50"
            assert result.total_successful == 100

    @pytest.mark.usefixtures("disable_gzip")
    def test_network_errors(self, toolkit_config: ToolkitClientConfig) -> None:
        """Test prevention of queue deadlocks under error conditions"""
        url = "https://test.com/api"

        def connection_error_callback(request):
            if '"id": 1' in request.body:
                # Fist worker
                raise requests.exceptions.ReadTimeout("Read timeout error")
            else:
                raise requests.exceptions.ConnectionError("Connection error")

        with (
            HTTPIterableProcessor(
                endpoint_url=url,
                config=toolkit_config,
                as_id=lambda item: item["id"],
                max_workers=2,
                batch_size=25,
                max_retries=2,  # Small max_retries to prevent test from running too long
            ) as processor,
            responses.RequestsMock() as rsps,
        ):
            rsps.add_callback(
                responses.POST,
                url,
                callback=connection_error_callback,
            )

            with patch("time.sleep"):  # Skip actual waiting
                items = [{"id": i} for i in range(50)]
                result = processor.process(items, total_items=len(items))

                # All items should eventually fail after retries
                assert result.total_failed == 50
                # Each item should have been attempted max_retries
                # With 50 items and batch size 25, that's 2 batches x max_retries
                assert len(rsps.calls) == 2 * processor.max_workers

    def test_worker_shutdown(self, toolkit_config):
        """Test that workers properly terminate when signaled"""
        url = "https://test.com/api"
        # For this test, we need to monitor the worker threads directly
        # We'll use a mock for the worker method to detect shutdown signals
        shutdown_signal_count = 0

        def mock_worker(work_queue, results_queue):
            nonlocal shutdown_signal_count
            while True:
                work_item = work_queue.get()
                if work_item is None:
                    shutdown_signal_count += 1
                    work_queue.task_done()
                    break
                results_queue.put(BatchResult(successful_items=[SuccessItem("id", 200) for _ in work_item.items]))
                work_queue.task_done()

        with (
            HTTPIterableProcessor(
                endpoint_url=url,
                config=toolkit_config,
                as_id=lambda item: item["id"],
                max_workers=4,
                batch_size=25,
            ) as processor,
            patch.object(processor, "_worker", side_effect=mock_worker),
        ):
            items = [{"id": i} for i in range(50)]
            processor.process(items, total_items=len(items))

            # Verify each worker received a shutdown signal
            assert shutdown_signal_count == processor.max_workers

    def test_no_access(self, processor: HTTPIterableProcessor) -> None:
        """Test handling of 401 Unauthorized responses"""
        with responses.RequestsMock() as rsps:
            rsps.add(
                responses.POST,
                processor.endpoint_url,
                status=401,
                json={"error": {"message": "Unauthorized access"}},
            )

            items = [{"id": i} for i in range(10)]
            result = processor.process(items, total_items=len(items))

            assert result.total_failed == 10
            assert result.total_successful == 0
            assert 401 in result.error_summary
            assert result.error_summary[401] == 10
            # Verify there was only one request attempt (no retries for 401)
            assert len(rsps.calls) == 1

    def test_rate_limit_handling(self, processor: HTTPIterableProcessor) -> None:
        """Test handling of 429 rate limit responses with eventual success"""
        items = [{"id": i} for i in range(10)]
        responses_sequence = [
            # First attempt gets rate limited
            {"status": 429, "json": {"error": {"message": "Too many requests"}}},
            # Second attempt succeeds
            {"status": 200, "json": {"items": items}},
        ]

        with responses.RequestsMock() as rsps:
            for resp in responses_sequence:
                rsps.add(
                    responses.POST,
                    processor.endpoint_url,
                    status=resp["status"],
                    json=resp["json"],
                )

            with patch("time.sleep"):  # Skip actual waiting
                result = processor.process(items, total_items=len(items))

                assert result.total_successful == 10
                assert result.total_failed == 0
                assert len(rsps.calls) == 2  # First fails, second succeeds

                # Verify rate limit was handled
                assert rsps.calls[0].response.status_code == 429
                assert rsps.calls[1].response.status_code == 200

    @pytest.mark.usefixtures("disable_gzip")
    def test_permanent_failure_single_item(self, toolkit_config: ToolkitClientConfig) -> None:
        """Test handling of permanent failure for a single item"""
        url = "https://test.com/api"
        # Track which items were sent in requests
        request_items = []

        def request_callback(request):
            payload = request.body.decode() if isinstance(request.body, bytes) else request.body
            # Extract the item ID from the request
            if '"id": 3' in payload:  # Item with ID 3 will fail
                return 400, {}, '{"error": {"message": "Invalid format for item"}}'
            else:
                request_items.append(payload)
                # Return payload for successful items
                return 200, {}, payload

        with (
            HTTPIterableProcessor(
                endpoint_url=url,
                config=toolkit_config,
                as_id=lambda item: item["id"],
                max_workers=2,
                batch_size=1,
            ) as processor,
            responses.RequestsMock() as rsps,
        ):
            rsps.add_callback(
                responses.POST,
                url,
                callback=request_callback,
            )

            items = [{"id": i} for i in range(5)]
            result = processor.process(items, total_items=len(items))

            # Verify one item failed and four succeeded
            assert result.total_successful == 4
            assert result.total_failed == 1
            assert 400 in result.error_summary
            assert result.error_summary[400] == 1

            # Verify the failed item has ID 3
            failed_id = result.failed_items[0].item
            assert failed_id == 3

            # Verify each item was processed individually
            assert len(rsps.calls) == 5

    def test_invalid_as_id_function(self, toolkit_config: ToolkitClientConfig) -> None:
        """Test that an error is raised if the as_id function is invalid"""

        def invalid_as_id(item) -> str:
            raise ValueError("Invalid ID function")

        with (
            responses.RequestsMock() as rsps,
            HTTPIterableProcessor[str](
                endpoint_url="https://test.com/api",
                config=toolkit_config,
                as_id=invalid_as_id,
                max_workers=2,
                batch_size=10,
            ) as processor,
        ):
            rsps.add(
                responses.POST,
                processor.endpoint_url,
                status=200,
                json={"items": []},
            )
            result = processor.process([{"id": 1}, {"id": 2}, {"id": 3}])

            assert len(result.unknown_items) == 3

    def test_empty_batch(self, processor: HTTPIterableProcessor) -> None:
        """Test that processing an empty batch returns an empty result"""
        result = processor.process([])

        assert result.total_successful == 0
        assert result.total_failed == 0
        assert result.total_processed == 0

    def test_raise_in_iteration(self, processor: HTTPIterableProcessor) -> None:
        """Test that an exception raised during iteration is handled correctly"""

        def items_generator():
            yield {"id": 1}
            yield {"id": 2}
            raise ValueError("Raising an error during iteration")

        with responses.RequestsMock() as rsps:
            rsps.add(
                responses.POST,
                processor.endpoint_url,
                status=200,
                json={"items": []},
            )
            result = processor.process(items_generator())

        assert str(result.producer_error) == "Raising an error during iteration"


class TestHTTPBatchProcessor:
    def test_happy_path(self, toolkit_config: ToolkitClientConfig) -> None:
        """Test that TestHTTPBatchProcessor processes items correctly"""
        url = "https://test.com/api"
        items = [{"id": i} for i in range(5)]

        batches: list[BatchResult[str]] = []

        def processor(batch: BatchResult[str]) -> None:
            batches.append(batch)

        with (
            responses.RequestsMock() as rsps,
            HTTPBatchProcessor[str](
                endpoint_url=url,
                config=toolkit_config,
                as_id=lambda item: item["id"],
                result_processor=processor,
                max_workers=2,
            ) as processor,
        ):
            rsps.add(
                responses.POST,
                url,
                status=200,
                json={"items": [{"id": 1}, {"id": 2}, {"id": 3}]},
            )
            rsps.add(
                responses.POST,
                url,
                status=200,
                json={"items": [{"id": 4}, {"id": 5}]},
            )
            processor.add_items(items[:3])
            processor.add_items(items[3:])

        assert len(batches) == 2

    def test_iterable_processor_empty_input(self, toolkit_config: ToolkitClientConfig) -> None:
        """Test that TestHTTPBatchProcessor does not call result_processor for empty input"""
        url = "https://test.com/api"
        batches: list[BatchResult[str]] = []

        def processor(batch: BatchResult[str]) -> None:
            batches.append(batch)

        with (
            responses.RequestsMock() as _,
            HTTPBatchProcessor[str](
                endpoint_url=url,
                config=toolkit_config,
                as_id=lambda item: item["id"],
                result_processor=processor,
                max_workers=2,
            ) as processor,
        ):
            processor.add_items([])
        assert len(batches) == 0

    def test_iterable_processor_result_processor_error(self, toolkit_config: ToolkitClientConfig) -> None:
        """Test that exceptions in result_processor are handled and logged"""
        url = "https://test.com/api"
        items = [{"id": i} for i in range(2)]
        error_logged = []

        def processor(batch: BatchResult[str]) -> None:
            raise RuntimeError("Test error in result processor")

        class DummyConsole:
            def print(self, msg):
                error_logged.append(msg)

        with (
            responses.RequestsMock() as rsps,
            HTTPBatchProcessor[str](
                endpoint_url=url,
                config=toolkit_config,
                as_id=lambda item: item["id"],
                result_processor=processor,
                max_workers=1,
                console=DummyConsole(),
            ) as processor,
        ):
            rsps.add(
                responses.POST,
                url,
                status=200,
                json={"items": [{"id": 1}, {"id": 2}]},
            )
            processor.add_items(items)
        assert any("Error processing result" in str(msg) for msg in error_logged)

    def test_iterable_processor_shutdown_no_items(self, toolkit_config: ToolkitClientConfig) -> None:
        """Test that threads shut down cleanly when no items are added"""
        url = "https://test.com/api"
        batches: list[BatchResult[str]] = []

        def processor(batch: BatchResult[str]) -> None:
            batches.append(batch)

        with HTTPBatchProcessor[str](
            endpoint_url=url,
            config=toolkit_config,
            as_id=lambda item: item["id"],
            result_processor=processor,
            max_workers=2,
        ) as processor:
            pass  # No items added
        assert len(batches) == 0

    def test_iterable_processor_thread_safety(self, toolkit_config: ToolkitClientConfig) -> None:
        """Test adding items from multiple threads is processed correctly"""
        url = "https://test.com/api"
        items = [{"id": i} for i in range(10)]
        batches: list[BatchResult[str]] = []

        def processor(batch: BatchResult[str]) -> None:
            batches.append(batch)

        with (
            responses.RequestsMock() as rsps,
            HTTPBatchProcessor[str](
                endpoint_url=url,
                config=toolkit_config,
                as_id=lambda item: item["id"],
                result_processor=processor,
                max_workers=2,
                batch_size=2,
            ) as processor,
        ):
            rsps.add(
                responses.POST,
                url,
                status=200,
                json={"items": [{"id": 1}, {"id": 2}]},
            )
            rsps.add(
                responses.POST,
                url,
                status=200,
                json={"items": [{"id": 3}, {"id": 4}]},
            )
            rsps.add(
                responses.POST,
                url,
                status=200,
                json={"items": [{"id": 5}, {"id": 6}]},
            )
            rsps.add(
                responses.POST,
                url,
                status=200,
                json={"items": [{"id": 7}, {"id": 8}]},
            )
            rsps.add(
                responses.POST,
                url,
                status=200,
                json={"items": [{"id": 9}, {"id": 10}]},
            )

            def add_items_thread(start, end):
                processor.add_items(items[start:end])

            threads = [threading.Thread(target=add_items_thread, args=(i, i + 2)) for i in range(0, 10, 2)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        assert len(batches) == 5
