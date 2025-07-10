import time
from unittest.mock import MagicMock, patch

import pytest
import requests
import responses
from cognite.client import global_config
from cognite.client.credentials import OAuthClientCredentials

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.utils.auth import CLIENT_NAME
from cognite_toolkit._cdf_tk.utils.batch_processor import HTTPBatchProcessor


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
def processor(toolkit_config: ToolkitClientConfig) -> HTTPBatchProcessor:
    return HTTPBatchProcessor(
        endpoint_url="https://test.com/api",
        config=toolkit_config,
        as_id=lambda item: item["id"],
        max_workers=4,
        batch_size=100,
    )


class TestHTTPBatchProcessor:
    def test_happy_path(self, toolkit_config: ToolkitClientConfig) -> None:
        url = "http://example.com/api"
        processor = HTTPBatchProcessor[str](
            url,
            toolkit_config,
            lambda item: item["externalId"],
        )
        with responses.RequestsMock() as rsps:
            rsps.add(
                responses.POST,
                url,
                status=200,
                json={"items": [{"externalId": "item1"}, {"externalId": "item2"}]},
            )
            result = processor.process(({"externalId": "item1"}, {"externalId": "item2"}))

            assert len(rsps.calls) == 1
            assert result.total_successful == 2

    def test_concurrent_processing(self, processor: HTTPBatchProcessor) -> None:
        """Test that items are processed concurrently using multiple workers"""
        with responses.RequestsMock() as rsps:
            # Add a callback to simulate delay and track request times
            request_times = []

            def request_callback(request):
                request_times.append(time.time())
                time.sleep(0.1)
                return (200, {}, '{"items": []}')

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
            assert result.total_successful == 400

    @pytest.mark.usefixtures("disable_gzip")
    def test_split_batch_concurrency(self, processor: HTTPBatchProcessor) -> None:
        """Test batch splitting under concurrent load"""
        processed_batch_counts = []

        def request_callback(request):
            # Extract the batch size from the request payload
            batch_count = request.body.count('"id":')
            processed_batch_counts.append(batch_count)

            # First batch (size 100) gets an error, others succeed
            if batch_count == 100:
                return (502, {}, '{"error": {"message": "Server Error"}}')
            else:
                return (200, {}, '{"items": []}')

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

    def test_network_errors(self, toolkit_config: ToolkitClientConfig) -> None:
        """Test prevention of queue deadlocks under error conditions"""
        url = "https://test.com/api"
        processor = HTTPBatchProcessor(
            endpoint_url=url,
            config=toolkit_config,
            as_id=lambda item: item["id"],
            max_workers=2,
            batch_size=25,
            max_retries=2,  # Small max_retries to prevent test from running too long
        )

        def connection_error_callback(request):
            raise requests.exceptions.ConnectionError("Connection error")

        with responses.RequestsMock() as rsps:
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
        processor = HTTPBatchProcessor(
            endpoint_url=url,
            config=toolkit_config,
            as_id=lambda item: item["id"],
            max_workers=4,
            batch_size=25,
        )

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
                work_queue.task_done()

        with patch.object(processor, "_worker", side_effect=mock_worker):
            items = [{"id": i} for i in range(50)]
            processor.process(items, total_items=len(items))

            # Verify each worker received a shutdown signal
            assert shutdown_signal_count == processor.max_workers
