import gzip
import json
import random
import socket
import sys
import threading
import time
from collections import Counter
from collections.abc import Callable, Hashable, Iterable, MutableMapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from itertools import zip_longest
from queue import Queue
from typing import Generic, Literal, TypeAlias, TypeVar

import requests
import urllib3
from cognite.client import global_config
from cognite.client.utils import _json
from requests.adapters import HTTPAdapter
from requests.structures import CaseInsensitiveDict
from rich.console import Console
from rich.progress import BarColumn, Progress, TaskProgressColumn, TextColumn, TimeRemainingColumn
from urllib3.util.retry import Retry

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.utils.auxiliary import get_current_toolkit_version, get_user_agent

from .collection import chunker
from .useful_types import JsonVal

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


T_ID = TypeVar("T_ID", bound=Hashable)
StatusCode: TypeAlias = int
Count: TypeAlias = int


@dataclass(frozen=True)
class FailedItem(Generic[T_ID]):
    item: T_ID
    status_code: int
    error_message: str


@dataclass(frozen=True)
class SuccessItem(Generic[T_ID]):
    item: T_ID
    status_code: int
    message: str | None = None
    response: dict[str, JsonVal] | None = None  # Optional response data from the server


@dataclass(frozen=True)
class BatchResult(Generic[T_ID]):
    successful_items: list[SuccessItem[T_ID]] = field(default_factory=list)
    failed_items: list[FailedItem[T_ID]] = field(default_factory=list)
    unknown_ids: list[FailedItem[str]] = field(default_factory=list)

    @property
    def total_items(self) -> int:
        return len(self.successful_items) + len(self.failed_items) + len(self.unknown_ids)


@dataclass(frozen=True)
class ProcessorResult(Generic[T_ID]):
    successful_items: list[SuccessItem[T_ID]] = field(default_factory=list)
    failed_items: list[FailedItem[T_ID]] = field(default_factory=list)
    unknown_items: list[FailedItem[str]] = field(default_factory=list)
    error_summary: dict[StatusCode, Count] = field(default_factory=dict)
    producer_error: Exception | None = None

    @property
    def total_successful(self) -> int:
        return len(self.successful_items)

    @property
    def total_failed(self) -> int:
        return len(self.failed_items)

    @property
    def total_processed(self) -> int:
        return self.total_successful + self.total_failed + len(self.unknown_items)

    @property
    def success_rate(self) -> float:
        return len(self.successful_items) / self.total_processed if self.total_processed > 0 else 0.0


@dataclass
class WorkItem:
    items: list[dict]
    connect_attempt: int = 0
    read_attempt: int = 0
    status_attempt: int = 0

    @property
    def total_attempts(self) -> int:
        return self.connect_attempt + self.read_attempt + self.status_attempt


class HTTPProcessor(Generic[T_ID]):
    """A generic HTTP processor for sending items to a specified endpoint in batches.

    This class handles rate limiting, retries, and error handling for HTTP requests.

    Args:
        endpoint_url (str): The URL of the endpoint to send requests to.
        config (ToolkitClientConfig): Configuration for the Toolkit client.
        as_id (Callable[[dict[str, JsonVal]], T_ID]): A function to convert an item to its ID.
        method (Literal["POST", "GET"]): HTTP method to use for requests, default is "POST".
        body_parameters (dict[str, JsonVal] | None): Additional parameters to include in the request body.
        batch_size (int): Number of items per batch, default is 1000.
        max_workers (int): Maximum number of worker threads, default is 8.
        max_retries (int): Maximum number of retries for failed requests, default is 10.
        console (Console | None): Optional console for output, defaults to a new Console instance.

    """

    def __init__(
        self,
        endpoint_url: str,
        config: ToolkitClientConfig,
        as_id: Callable[[dict[str, JsonVal]], T_ID],
        method: Literal["POST", "GET"] = "POST",
        body_parameters: dict[str, JsonVal] | None = None,
        batch_size: int = 1_000,
        max_workers: int = 8,
        max_retries: int = 10,
        console: Console | None = None,
    ):
        self.endpoint_url = endpoint_url
        self.method = method.upper()
        self.body_args = body_parameters
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.console = console or Console()
        self.as_id = as_id

        self._config = config

        # Thread-safe session for connection pooling
        self.session = self._create_thread_safe_session()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: object | None
    ) -> Literal[False]:
        """Close the session when exiting the context."""
        self.session.close()
        return False  # Do not suppress exceptions

    def _create_thread_safe_session(self) -> requests.Session:
        session = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=self.max_workers,
            pool_maxsize=self.max_workers * 2,
            max_retries=Retry(total=0),  # We handle retries manually
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _create_headers(self) -> MutableMapping[str, str]:
        headers: MutableMapping[str, str] = CaseInsensitiveDict()
        headers.update(requests.utils.default_headers())
        auth_name, auth_value = self._config.credentials.authorization_header()
        headers[auth_name] = auth_value
        headers["content-type"] = "application/json"
        headers["accept"] = "application/json"
        headers["x-cdp-sdk"] = f"CogniteToolkit:{get_current_toolkit_version()}"
        headers["x-cdp-app"] = self._config.client_name
        headers["cdf-version"] = self._config.api_subversion
        if "User-Agent" in headers:
            headers["User-Agent"] += f" {get_user_agent()}"
        else:
            headers["User-Agent"] = get_user_agent()
        if not global_config.disable_gzip:
            headers["Content-Encoding"] = "gzip"
        return headers

    def _prepare_payload(self, work_item: WorkItem) -> str | bytes:
        """
        Prepare the payload for the HTTP request.
        This method should be overridden in subclasses to customize the payload format.
        """
        data: str | bytes
        try:
            data = _json.dumps({"items": work_item.items, **(self.body_args or {})}, allow_nan=False)
        except ValueError as e:
            # A lot of work to give a more human friendly error message when nans and infs are present:
            msg = "Out of range float values are not JSON compliant"
            if msg in str(e):  # exc. might e.g. contain an extra ": nan", depending on build (_json.make_encoder)
                raise ValueError(f"{msg}. Make sure your data does not contain NaN(s) or +/- Inf!").with_traceback(
                    e.__traceback__
                ) from None
            raise

        if not global_config.disable_gzip:
            data = gzip.compress(data.encode())
        return data

    def _worker(
        self,
        work_queue: Queue,
        results_queue: Queue,
    ) -> None:
        while (work_item := work_queue.get()) is not None:
            try:
                response = self._make_request(work_item)
                self._handle_response(response, work_item, work_queue, results_queue)
            except Exception as e:
                self._handle_error(e, work_item, work_queue, results_queue)
            finally:
                work_queue.task_done()
        work_queue.task_done()

    def _make_request(self, work_item: WorkItem) -> requests.Response:
        headers = self._create_headers()
        data = self._prepare_payload(work_item)
        return self.session.request(
            self.method,
            self.endpoint_url,
            data=data,
            headers=headers,
            timeout=self._config.timeout,
            allow_redirects=False,
        )

    def _handle_response(
        self, response: requests.Response, work_item: WorkItem, work_queue: Queue, results_queue: Queue
    ) -> None:
        if 200 <= response.status_code < 300:
            results_queue.put(self._create_success_batch_result(work_item.items, response=response))
        elif response.status_code in {401, 403}:
            self.console.print("[red]Unauthorized request. Please check your credentials.[/red]")
            results_queue.put(self._create_failed_batch_result(work_item.items, response.status_code, response.text))
        elif response.status_code in {400, 409, 422, 502, 503, 504} and len(work_item.items) > 1:
            # 400, 409, 422: There is at least one item that is invalid, split the batch to get all valid items processed
            # 502, 503, 504: Server errors, retry in smaller batches
            mid = len(work_item.items) // 2
            attempts = (
                work_item.status_attempt + 1 if response.status_code in {502, 503, 504} else work_item.status_attempt
            )
            work_queue.put(
                WorkItem(
                    items=work_item.items[:mid],
                    status_attempt=attempts,
                    connect_attempt=work_item.connect_attempt,
                    read_attempt=work_item.read_attempt,
                )
            )
            work_queue.put(
                WorkItem(
                    items=work_item.items[mid:],
                    status_attempt=attempts,
                    connect_attempt=work_item.connect_attempt,
                    read_attempt=work_item.read_attempt,
                )
            )
        elif work_item.status_attempt < self.max_retries and response.status_code in {429, 502, 503, 504}:
            work_item.status_attempt += 1
            time.sleep(self._backoff_time(work_item.total_attempts))
            work_queue.put(work_item)
        else:
            # Permanent failure
            error = response.text
            try:
                body = response.json()
                if "error" in body and "message" in body["error"]:
                    error = body["error"]["message"]
            except ValueError:
                # If the response is not JSON, we keep the original text
                pass
            results_queue.put(self._create_failed_batch_result(work_item.items, response.status_code, error))

    @staticmethod
    def _backoff_time(attempts: int) -> float:
        backoff_time = 0.5 * (2**attempts)
        return min(backoff_time, global_config.max_retry_backoff) * random.uniform(0, 1.0)

    def _handle_error(
        self,
        e: Exception,
        work_item: WorkItem,
        work_queue: Queue,
        results_queue: Queue,
    ) -> None:
        if self._any_exception_in_context_isinstance(
            e, (socket.timeout, urllib3.exceptions.ReadTimeoutError, requests.exceptions.ReadTimeout)
        ):
            error_type = "read"
            work_item.read_attempt += 1
            attempts = work_item.read_attempt
        elif self._any_exception_in_context_isinstance(
            e,
            (
                ConnectionError,
                urllib3.exceptions.ConnectionError,
                urllib3.exceptions.ConnectTimeoutError,
                requests.exceptions.ConnectionError,
            ),
        ):
            error_type = "connect"
            work_item.connect_attempt += 1
            attempts = work_item.connect_attempt
        else:
            error_msg = f"Unexpected exception: {e!s}"
            results_queue.put(self._create_failed_batch_result(work_item.items, 0, error_msg))
            return

        if attempts < self.max_retries:
            time.sleep(self._backoff_time(work_item.total_attempts))
            work_queue.put(work_item)
        else:
            error_msg = f"RequestException after {self.max_retries} {error_type} attempts: {e!s}"
            results_queue.put(self._create_failed_batch_result(work_item.items, 0, error_msg))

    def _create_failed_batch_result(
        self,
        items: list[dict[str, JsonVal]],
        status_code: int,
        error_message: str,
    ) -> BatchResult[T_ID]:
        failed_items: list[FailedItem[T_ID]] = []
        unknown_items: list[FailedItem[str]] = []
        for item in items:
            try:
                item_id = self.as_id(item)
            except Exception as e:
                unknown_items.append(self._create_unknown_item(item, status_code, error_message, e))
            else:
                failed_items.append(FailedItem(item_id, status_code, error_message))
        return BatchResult(failed_items=failed_items, unknown_ids=unknown_items)

    def _create_success_batch_result(
        self, items: list[dict[str, JsonVal]], response: requests.Response, message: str | None = None
    ) -> BatchResult[T_ID]:
        status_code = response.status_code
        response_items = self._parse_response_items(response)
        has_printed_warning = False
        success_items: list[SuccessItem[T_ID]] = []
        unknown_items: list[FailedItem[str]] = []
        failed_items: list[FailedItem[T_ID]] = []
        for item, response_item in zip_longest(items, response_items, fillvalue=None):
            if item is None:
                if not has_printed_warning:
                    self.console.print("[red]Got more response 'items' than request items.[/red]")
                    has_printed_warning = True
                continue

            try:
                item_id = self.as_id(item)
            except Exception as e:
                unknown_items.append(self._create_unknown_item(item, status_code, message, e))
            else:
                if response_item is None:
                    failed_items.append(
                        FailedItem(item=item_id, status_code=status_code, error_message="Response item is None")
                    )
                else:
                    success_items.append(
                        SuccessItem(item=item_id, response=response_item, status_code=status_code, message=message)
                    )
        return BatchResult(successful_items=success_items, unknown_ids=unknown_items, failed_items=failed_items)

    def _parse_response_items(self, response: requests.Response) -> list[dict[str, JsonVal]]:
        try:
            response_items = response.json().get("items", [])
        except (json.JSONDecodeError, TypeError) as e:
            self.console.print(f"[red]Failed to decode JSON response: {e!s}[/red]")
            response_items = []
        if not isinstance(response_items, list):
            self.console.print("[red]Response 'items' is not a list. Skipping response.[/red]")
            response_items = []
        return response_items

    @staticmethod
    def _create_unknown_item(
        item: dict[str, JsonVal], status_code: int, error_message: str | None, exception: Exception
    ) -> FailedItem[str]:
        try:
            item_repr = str(item)[:50]
        except Exception:
            item_repr = "<unrepresentable item>"
        return FailedItem(
            f"as_id failed for item {item_repr} error {exception!s}.", status_code, error_message or "as_id failed"
        )

    @staticmethod
    def _aggregate_results(results: list[BatchResult[T_ID]], producer_error: Exception | None) -> ProcessorResult[T_ID]:
        successful_items = [item for r in results for item in r.successful_items]
        failed_items = [item for r in results for item in r.failed_items]
        unknown_ids = [item for r in results for item in r.unknown_ids]
        error_summary: dict[int, int] = Counter()
        for item in failed_items:
            error_summary[item.status_code] += 1
        for unknown_item in unknown_ids:
            error_summary[unknown_item.status_code] += 1

        return ProcessorResult(
            unknown_items=unknown_ids,
            successful_items=successful_items,
            failed_items=failed_items,
            error_summary=error_summary,
            producer_error=producer_error,
        )

    @classmethod
    def _any_exception_in_context_isinstance(
        cls, exc: BaseException, exc_types: tuple[type[BaseException], ...] | type[BaseException]
    ) -> bool:
        """requests does not use the "raise ... from ..." syntax, so we need to access the underlying exceptions using
        the __context__ attribute.
        """
        if isinstance(exc, exc_types):
            return True
        if exc.__context__ is None:
            return False
        return cls._any_exception_in_context_isinstance(exc.__context__, exc_types)


class HTTPIterableProcessor(HTTPProcessor[T_ID]):
    """A generic HTTP batch processor for sending items to a specified endpoint from an iterable source.

    This class handles batching, rate limiting, retries, and error handling for HTTP requests.

    Args:
        endpoint_url (str): The URL of the endpoint to send requests to.
        config (ToolkitClientConfig): Configuration for the Toolkit client.
        as_id (Callable[[dict[str, JsonVal]], T_ID]): A function to convert an item to its ID.
        method (Literal["POST", "GET"]): HTTP method to use for requests, default is "POST".
        body_parameters (dict[str, JsonVal] | None): Additional parameters to include in the request body.
        batch_size (int): Number of items per batch, default is 1000.
        max_workers (int): Maximum number of worker threads, default is 8.
        max_retries (int): Maximum number of retries for failed requests, default is 10.
        console (Console | None): Optional console for output, defaults to a new Console instance.
        description (str): Description for the progress bar, default is "Processing items".

    """

    def __init__(
        self,
        endpoint_url: str,
        config: ToolkitClientConfig,
        as_id: Callable[[dict[str, JsonVal]], T_ID],
        method: Literal["POST", "GET"] = "POST",
        body_parameters: dict[str, JsonVal] | None = None,
        batch_size: int = 1_000,
        max_workers: int = 8,
        max_retries: int = 10,
        console: Console | None = None,
        description: str = "Processing items",
    ):
        super().__init__(
            endpoint_url=endpoint_url,
            config=config,
            as_id=as_id,
            method=method,
            body_parameters=body_parameters,
            batch_size=batch_size,
            max_workers=max_workers,
            max_retries=max_retries,
            console=console,
        )
        self._produced_count = 0
        self._process_exception: Exception | None = None
        self.description = description

    def _producer(self, items_iterator: Iterable[dict[str, JsonVal]], work_queue: Queue) -> None:
        batch: list[dict] = []
        try:
            for item in items_iterator:
                batch.append(item)
                if len(batch) >= self.batch_size:
                    while work_queue.qsize() >= self.max_workers * 2:
                        # Wait for space in the queue
                        time.sleep(0.1)
                    work_queue.put(WorkItem(items=batch))
                    self._produced_count += len(batch)
                    batch = []
            if batch:
                work_queue.put(WorkItem(items=batch))
                self._produced_count += len(batch)
        except Exception as e:
            self.console.print(f"[red]Error in producer thread: {e!s}[/red]")
            # If an error occurs, we still want to put the items in the queue to avoid losing them
            if batch:
                work_queue.put(WorkItem(items=batch))
                self._produced_count += len(batch)
            self._process_exception = e

    def process(self, items: Iterable[dict[str, JsonVal]], total_items: int | None = None) -> ProcessorResult[T_ID]:
        """Process items in batches using multiple worker threads.

        Args:
            items (Iterable[dict[str, JsonVal]]): An iterable of items to process.
            total_items (int | None): Total number of items to process, used for progress tracking. The tracking
                will only show number of items processed, not the number of items in the queue.

        Returns:
            ProcessorResult[T_ID]: The result of processing the items, including successful and failed items.

        """
        work_queue: Queue[WorkItem | None] = Queue()
        results_queue: Queue[BatchResult[T_ID]] = Queue()

        self._produced_count = 0
        self._process_exception = None
        producer_thread = threading.Thread(target=self._producer, args=(items, work_queue), daemon=True)
        producer_thread.start()
        batch_results: list[BatchResult[T_ID]] = []
        with Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TextColumn("{task.fields[processed_items]} items processed"),
            TimeRemainingColumn(),
            console=self.console,
        ) as progress:
            task = progress.add_task(self.description, total=total_items, processed_items=0)
            with ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="batch_worker") as executor:
                # Start workers
                for _ in range(self.max_workers):
                    executor.submit(self._worker, work_queue, results_queue)

                producer_thread.join()
                if self._produced_count == 0:
                    self.console.print("[yellow]No items to process.[/yellow]")
                else:
                    work_queue.join()
                    processed_count = 0
                    while processed_count < self._produced_count:
                        result = results_queue.get()
                        batch_results.append(result)
                        processed_count += result.total_items
                        progress.update(task, processed_items=processed_count)
                for _ in range(self.max_workers):
                    work_queue.put(None)
        return self._aggregate_results(batch_results, self._process_exception)


class HTTPBatchProcessor(HTTPProcessor[T_ID]):
    """An HTTP processor for processing items in batches.

    This class handles batching, rate limiting, retries, and error handling for HTTP requests.

    Args:
        endpoint_url (str): The URL of the endpoint to send requests to.
        config (ToolkitClientConfig): Configuration for the Toolkit client.
        as_id (Callable[[dict[str, JsonVal]], T_ID]): A function to convert an item to its ID.
        result_processor (Callable[[BatchResult[T_ID]], None]): A callable that processes the result of each batch.
        method (Literal["POST", "GET"]): HTTP method to use for requests, default is "POST".
        body_parameters (dict[str, JsonVal] | None): Additional parameters to include in the request body.
        batch_size (int): Number of items per batch, default is 1000.
        max_workers (int): Maximum number of worker threads, default is 8.
        max_retries (int): Maximum number of retries for failed requests, default is 10.
        console (Console | None): Optional console for output, defaults to a new Console instance.

    """

    def __init__(
        self,
        endpoint_url: str,
        config: ToolkitClientConfig,
        as_id: Callable[[dict[str, JsonVal]], T_ID],
        result_processor: Callable[[BatchResult[T_ID]], None],
        method: Literal["POST", "GET"] = "POST",
        body_parameters: dict[str, JsonVal] | None = None,
        batch_size: int = 1_000,
        max_workers: int = 8,
        max_retries: int = 10,
        console: Console | None = None,
    ):
        super().__init__(
            endpoint_url=endpoint_url,
            config=config,
            as_id=as_id,
            method=method,
            body_parameters=body_parameters,
            batch_size=batch_size,
            max_workers=max_workers,
            max_retries=max_retries,
            console=console,
        )
        self.result_processor = result_processor
        self._work_queue: Queue[WorkItem | None] | None = None
        self._result_queue: Queue[BatchResult[T_ID] | None] | None = None
        self._worker_threads: list[threading.Thread] = []
        self._result_thread: threading.Thread | None = None

    def __enter__(self) -> Self:
        """Enter the context manager, initializing the work and result queues."""
        try:
            # Limiting the queue size to avoid excessive memory usage
            self._work_queue = Queue(self.max_workers * 2)
            self._result_queue = Queue()
            self._worker_threads = [
                threading.Thread(target=self._worker, args=(self._work_queue, self._result_queue), daemon=True)
                for _ in range(self.max_workers)
            ]
            self._result_thread = threading.Thread(target=self._result_processor, daemon=True)
            self._result_thread.start()
            for thread in self._worker_threads:
                thread.start()
        except (RuntimeError, OSError) as e:
            self.console.print(f"[red]Error initializing processor: {e!s}[/red]")
            self._stop()
            raise RuntimeError("Failed to initialize the processor.") from e
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: object | None
    ) -> Literal[False]:
        """Exit the context manager, stopping the worker threads and closing the queues."""
        self._stop()
        return False

    def add_items(self, items: Iterable[dict[str, JsonVal]]) -> None:
        """Add items to the processor for processing.

        Args:
            items (Iterable[dict[str, JsonVal]]): An iterable of items to process.
        """
        if self._work_queue is None:
            raise RuntimeError("Processor is not initialized. Please use the context manager to initialize it.")
        for chunk in chunker(items, self.batch_size):
            self._work_queue.put(WorkItem(items=chunk))

    def _stop(self) -> None:
        """Stop the processor, joining all worker threads and closing the queues."""
        if self._work_queue is not None:
            for _ in self._worker_threads:
                self._work_queue.put(None)
            for thread in self._worker_threads:
                thread.join()
            self._work_queue = None
        if self._result_queue is not None:
            self._result_queue.put(None)
            if self._result_thread and self._result_thread.is_alive():
                self._result_thread.join()
            self._result_queue = None

    def _result_processor(self) -> None:
        """Process results from the result queue and aggregate them."""
        if self._result_queue is None:
            raise RuntimeError("Result queue is not initialized. Please use the context manager to initialize it.")
        while (result := self._result_queue.get()) is not None:
            try:
                self.result_processor(result)
            except Exception as e:
                # The result processor is user-defined, so we need to catch any exceptions
                self.console.print(f"[red]Error processing result: {e!s}[/red]")
                self.console.print_exception()
            finally:
                self._result_queue.task_done()
        self._result_queue.task_done()
