import gzip
import random
import socket
import threading
import time
from collections import Counter
from collections.abc import Callable, Hashable, Iterable, MutableMapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from queue import Queue
from typing import Generic, Literal, TypeVar

import requests
import urllib3
from cognite.client import global_config
from cognite.client.utils import _json
from cognite.client.utils._auxiliary import get_user_agent
from requests.adapters import HTTPAdapter
from requests.structures import CaseInsensitiveDict
from rich.console import Console
from rich.progress import BarColumn, Progress, TaskProgressColumn, TextColumn, TimeRemainingColumn
from urllib3.util.retry import Retry

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.utils.auxiliary import get_current_toolkit_version

from .useful_types import JsonVal

T_ID = TypeVar("T_ID", bound=Hashable)


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


@dataclass(frozen=True)
class BatchResult(Generic[T_ID]):
    successful_items: list[SuccessItem[T_ID]] = field(default_factory=list)
    failed_items: list[FailedItem[T_ID]] = field(default_factory=list)

    @property
    def total_items(self) -> int:
        return len(self.successful_items) + len(self.failed_items)


@dataclass(frozen=True)
class ProcessorResult(Generic[T_ID]):
    total_successful: int
    total_failed: int
    successful_items: list[SuccessItem[T_ID]] = field(default_factory=list)
    failed_items: list[FailedItem[T_ID]] = field(default_factory=list)
    error_summary: dict[int, int] = field(default_factory=dict)  # status_code -> count

    @property
    def total_processed(self) -> int:
        return self.total_successful + self.total_failed

    @property
    def success_rate(self) -> float:
        return self.total_successful / self.total_processed if self.total_processed > 0 else 0.0


@dataclass
class WorkItem:
    items: list[dict]
    connect_attempt: int = 0
    read_attempt: int = 0
    status_attempt: int = 0

    @property
    def total_attempts(self) -> int:
        return self.connect_attempt + self.read_attempt + self.status_attempt


class HTTPBatchProcessor(Generic[T_ID]):
    """A generic HTTP batch processor for sending items to a specified endpoint in batches.

    This class handles batching, rate limiting, retries, and error handling for HTTP requests.

    Args:
        endpoint_url (str): The URL of the endpoint to send requests to.
        config (ToolkitClientConfig): Configuration for the Toolkit client.
        as_id (Callable[[dict], T_ID]): A function to convert an item to its ID.
        method (Literal["POST", "GET"]): HTTP method to use for requests, default is "POST".
        body_parameters (dict[str, object] | None): Additional parameters to include in the request body.
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
        as_id: Callable[[dict], T_ID],
        method: Literal["POST", "GET"] = "POST",
        body_parameters: dict[str, object] | None = None,
        batch_size: int = 1_000,
        max_workers: int = 8,
        max_retries: int = 10,
        console: Console | None = None,
        description: str = "Processing items",
    ):
        self.endpoint_url = endpoint_url
        self.method = method.upper()
        self.body_args = body_parameters
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.console = console or Console()
        self.as_id = as_id
        self.description = description

        self._config = config

        # Thread-safe session for connection pooling
        self.session = self._create_thread_safe_session()

        # Shared state for rate limiting and backoff
        self._rate_limit_lock = threading.Lock()
        self._rate_limit_until = 0.0
        self._token_expiry = 0.0

    def __enter__(self) -> "HTTPBatchProcessor[T_ID]":
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

    def _handle_rate_limit(self) -> None:
        with self._rate_limit_lock:
            backoff = 1.0 + random.uniform(0, 1)
            self.console.print(f"[yellow]Rate limit hit (429). Backing off for {backoff:.2f}s.[/yellow]")
            self._rate_limit_until = time.time() + backoff

    def _producer(self, items_iterator: Iterable[dict[str, JsonVal]], work_queue: Queue) -> None:
        batch: list[dict] = []
        for item in items_iterator:
            batch.append(item)
            if len(batch) >= self.batch_size:
                work_queue.put(WorkItem(items=batch))
                batch = []
        if batch:
            work_queue.put(WorkItem(items=batch))

    def process(self, items: Iterable[dict[str, JsonVal]], total_items: int | None = None) -> ProcessorResult[T_ID]:
        work_queue: Queue[WorkItem | None] = Queue(maxsize=self.max_workers * 2)
        results_queue: Queue[BatchResult[T_ID]] = Queue()

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

                processed_count = 0
                while producer_thread.is_alive() or not work_queue.empty():
                    # Process results as they come in
                    while not results_queue.empty():
                        result = results_queue.get()
                        batch_results.append(result)
                        processed_count += result.total_items
                        progress.update(task, processed_items=processed_count)
                        results_queue.task_done()
                    time.sleep(0.1)  # Prevent busy-waiting

                # Wait for all tasks in the queue (including retries/splits) to be processed.
                work_queue.join()

                # Final drain of the results queue
                while not results_queue.empty():
                    result = results_queue.get()
                    batch_results.append(result)
                    processed_count += result.total_items
                    progress.update(task, processed_items=processed_count)
                    results_queue.task_done()

                # Shut down workers gracefully
                for _ in range(self.max_workers):
                    work_queue.put(None)

        return self._aggregate_results(batch_results)

    def _worker(
        self,
        work_queue: Queue,
        results_queue: Queue,
    ) -> None:
        while (work_item := work_queue.get()) is not None:
            try:
                with self._rate_limit_lock:
                    backoff_time = self._rate_limit_until - time.time()
                if backoff_time > 0:
                    jitter = random.uniform(0, 0.2)
                    time.sleep(backoff_time + jitter)

                response = self._make_request(work_item)
                self._handle_response(response, work_item, work_queue, results_queue)
            except Exception as e:
                self._handle_network_error(e, work_item, work_queue, results_queue)
            finally:
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
            results_queue.put(
                BatchResult(
                    successful_items=[
                        SuccessItem(item=self.as_id(item), status_code=response.status_code) for item in work_item.items
                    ]
                )
            )
        elif response.status_code in {401, 403}:
            self.console.print("[red]Unauthorized request. Please check your credentials.[/red]")
            failed = [FailedItem(self.as_id(item), response.status_code, response.text) for item in work_item.items]
            results_queue.put(BatchResult(failed_items=failed))
        elif response.status_code in {400, 409, 422, 502, 503, 504} and len(work_item.items) > 1:
            # 400, 409, 422: There is at least one item that is invalid, split the batch to get all valid items processed
            # 502, 503, 504: Server errors, retry in smaller batches
            mid = len(work_item.items) // 2
            attempts = (
                work_item.status_attempt + 1 if response.status_code in {502, 503, 504} else work_item.status_attempt
            )
            work_queue.put(WorkItem(items=work_item.items[:mid], status_attempt=attempts))
            work_queue.put(WorkItem(items=work_item.items[mid:], status_attempt=attempts))
        elif work_item.status_attempt <= self.max_retries and response.status_code in {429, 502, 503, 504}:
            if response.status_code == 429:
                self._handle_rate_limit()
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
            failed = [FailedItem(self.as_id(item), response.status_code, error) for item in work_item.items]
            results_queue.put(BatchResult(failed_items=failed))

    @staticmethod
    def _backoff_time(attempts: int) -> float:
        backoff_time = 0.5 * (2**attempts)
        return min(backoff_time, global_config.max_retry_backoff) * random.uniform(0, 1.0)

    def _handle_network_error(
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
            failed = [FailedItem(self.as_id(item), 0, error_msg) for item in work_item.items]
            results_queue.put(BatchResult(failed_items=failed))
            return

        if attempts < self.max_retries:
            time.sleep(self._backoff_time(attempts))
            work_queue.put(work_item)
        else:
            error_msg = f"RequestException after {self.max_retries} {error_type} attempts: {e!s}"
            failed = [FailedItem(self.as_id(item), 0, error_msg) for item in work_item.items]
            results_queue.put(BatchResult(failed_items=failed))

    @staticmethod
    def _aggregate_results(results: list[BatchResult[T_ID]]) -> ProcessorResult[T_ID]:
        successful_items = [item for r in results for item in r.successful_items]
        failed_items = [item for r in results for item in r.failed_items]
        error_summary: dict[int, int] = Counter()
        for item in failed_items:
            error_summary[item.status_code] += 1

        return ProcessorResult(
            total_successful=len(successful_items),
            total_failed=len(failed_items),
            successful_items=successful_items,
            failed_items=failed_items,
            error_summary=error_summary,
        )

    def __del__(self) -> None:
        return self.session.close()

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
