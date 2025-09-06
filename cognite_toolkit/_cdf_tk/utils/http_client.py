import gzip
import json
import random
import socket
import sys
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, MutableMapping
from dataclasses import dataclass, field
from itertools import zip_longest
from queue import Queue
from typing import Literal

import requests
import urllib3
from cognite.client import global_config
from cognite.client.utils import _json
from requests.adapters import HTTPAdapter
from requests.structures import CaseInsensitiveDict
from rich.console import Console
from urllib3.util.retry import Retry

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.utils.auxiliary import get_current_toolkit_version, get_user_agent

from .collection import chunker
from .useful_types import JsonVal

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


@dataclass(frozen=True)
class BatchResponseData: ...


@dataclass
class BatchRequestData(ABC):
    url: str
    method: Literal["POST", "GET", "PUT"]
    connect_attempt: int = 0
    read_attempt: int = 0
    status_attempt: int = 0

    @abstractmethod
    def dump_body(self) -> dict[str, JsonVal]:
        raise NotImplementedError()

    @property
    def total_attempts(self) -> int:
        return self.connect_attempt + self.read_attempt + self.status_attempt


@dataclass
class ItemBatchRequestData(BatchRequestData):
    items: list[dict[str, JsonVal]] = field(default_factory=list)
    extra_body_args: dict[str, JsonVal] | None = None

    def dump_body(self) -> dict[str, JsonVal]:
        return {"items": self.items, **self.extra_body_args}


@dataclass
class SingleRequestData(BatchRequestData):
    body: dict[str, JsonVal] | None = None

    def dump_body(self) -> dict[str, JsonVal]:
        return self.body or {}


class HTTPClient:
    """A generic HTTP Client for sending requests concurrently.

    This class handles rate limiting, retries, and error handling for HTTP requests.

    Args:
        config (ToolkitClientConfig): Configuration for the Toolkit client.
        max_workers (int): Maximum number of worker threads, default is 8.
        max_retries (int): Maximum number of retries for failed requests, default is 10.
        result_processor (Callable[[BatchResponse, None]] | None): Optional callable to process results.
        console (Console | None): Optional console for output, defaults to a new Console instance.

    """

    def __init__(
        self,
        config: ToolkitClientConfig,
        max_workers: int = 8,
        max_retries: int = 10,
        result_processor: Callable[[BatchResponseData], None] | None = None,
        console: Console | None = None,
    ):
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.result_processor = result_processor
        self.console = console or Console()
        self.config = config
        self._session: requests.Session | None = None
        self._work_queue: Queue[BatchRequestData | None] | None = None
        self._result_queue: Queue[BatchResponseData | None] | None = None
        self._worker_threads: list[threading.Thread] = []
        self._result_thread: threading.Thread | None = None

    def __enter__(self) -> Self:
        try:
            self._session = self._create_thread_safe_session()
            # Limiting the queue size to avoid excessive memory usage
            self._work_queue = Queue(self.max_workers * 2)
            self._result_queue = Queue()
            self._worker_threads = [
                threading.Thread(target=self._worker, args=(self._work_queue, self._result_queue), daemon=True)
                for _ in range(self.max_workers)
            ]
            for thread in self._worker_threads:
                thread.start()
        except (RuntimeError, OSError) as e:
            self.console.print(f"[red]Error initializing processor: {e!s}[/red]")
            self._stop()
            raise RuntimeError("Failed to initialize the processor.") from e
        return self

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
            self._result_queue = None

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: object | None
    ) -> Literal[False]:
        """Close the session when exiting the context."""
        self._session.close()
        self._stop()
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
        auth_name, auth_value = self.config.credentials.authorization_header()
        headers[auth_name] = auth_value
        headers["content-type"] = "application/json"
        headers["accept"] = "application/json"
        headers["x-cdp-sdk"] = f"CogniteToolkit:{get_current_toolkit_version()}"
        headers["x-cdp-app"] = self.config.client_name
        headers["cdf-version"] = self.config.api_subversion
        if "User-Agent" in headers:
            headers["User-Agent"] += f" {get_user_agent()}"
        else:
            headers["User-Agent"] = get_user_agent()
        if not global_config.disable_gzip:
            headers["Content-Encoding"] = "gzip"
        return headers

    def request(
        self,
        endpoint_url: str,
        method: Literal["POST", "GET", "PUT"],
        body: dict[str, JsonVal] | None = None,
    ) -> requests.Response:
        """Makes a single HTTP request in the current thread."""
        return self._make_request(SingleRequestData(url=endpoint_url, method=method, body=body or {}))

    def request_items(
        self,
        items: Iterable[dict[str, JsonVal]],
        endpoint_url: str,
        method: Literal["POST", "GET", "PUT"],
        extra_body_args: dict[str, JsonVal] | None = None,
        batch_size: int = 1_000,
    ) -> None:
        """Makes multiple HTTP request concurrently using worker thread.

        Args:
            items (Iterable[dict[str, JsonVal]]): An iterable of items to be sent in the request body.
            endpoint_url (str): The endpoint URL to send the requests to.
            method (Literal["POST", "GET", "PUT"]): The HTTP method to use.
            extra_body_args (dict[str, JsonVal] | None): Additional arguments to include in the request body.
            batch_size (int): The number of items to include in each batch request
        """
        if self._work_queue is None:
            raise RuntimeError("Processor is not initialized. Please use the context manager to initialize it.")
        for chunk in chunker(items, batch_size):
            self._work_queue.put(
                ItemBatchRequestData(
                    url=endpoint_url, method=method, items=list(chunk), extra_body_args=extra_body_args
                )
            )

    @staticmethod
    def _prepare_payload(request: BatchRequestData) -> str | bytes:
        """
        Prepare the payload for the HTTP request.
        This method should be overridden in subclasses to customize the payload format.
        """
        data: str | bytes
        try:
            data = _json.dumps(request.dump_body(), allow_nan=False)
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
        while (request := work_queue.get()) is not None:
            result: BatchRequestData | BatchResponseData | None = None
            try:
                response = self._make_request(request)
                result = self._handle_response(response, request)
            except Exception as e:
                result = self._handle_error(e, request)
            finally:
                work_queue.task_done()
            if isinstance(result, BatchRequestData):
                work_queue.put(result)
            elif isinstance(result, BatchResponseData):
                results_queue.put(result)
        work_queue.task_done()

    def _make_request(self, request: BatchRequestData) -> requests.Response:
        headers = self._create_headers()
        data = self._prepare_payload(request)
        return self._session.request(
            request.method,
            request.url,
            data=data,
            headers=headers,
            timeout=self.config.timeout,
            allow_redirects=False,
        )

    def _handle_response(
        self, response: requests.Response, work_item: BatchRequestData
    ) -> BatchRequestData | BatchResponseData:
        if 200 <= response.status_code < 300:
            return self._create_success_batch_result(work_item.items, response=response)
        elif response.status_code in {401, 403}:
            self.console.print("[red]Unauthorized request. Please check your credentials.[/red]")
            return self._create_failed_batch_result(work_item.items, response.status_code, response.text)
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
            return self._create_failed_batch_result(work_item.items, response.status_code, error)

    @staticmethod
    def _backoff_time(attempts: int) -> float:
        backoff_time = 0.5 * (2**attempts)
        return min(backoff_time, global_config.max_retry_backoff) * random.uniform(0, 1.0)

    def _handle_error(self, e: Exception, work_item: BatchRequestData) -> BatchRequestData | BatchResponseData:
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
    ) -> BatchResponseData:
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
    ) -> BatchResponseData:
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
