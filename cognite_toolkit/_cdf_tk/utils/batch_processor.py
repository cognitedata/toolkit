import json
import random
import threading
import time
from collections import Counter
from collections.abc import Callable, Iterable, Hashable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from queue import Empty, Queue
from typing import Any, Generic, TypeVar, Literal

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from rich.console import Console
from rich.progress import Progress, BarColumn, TaskProgressColumn, TextColumn, TimeRemainingColumn

T_ID = TypeVar("T_ID", bound=Hashable)

@dataclass(frozen=True)
class FailedItem(Generic[T_ID]):
    item: T_ID
    status_code: int
    error_message: str

@dataclass(frozen=True)
class SuccessItem(Generic[T_ID]):
    item: T_ID
    operation: Literal["create", "update", "delete"]

@dataclass(frozen=True)
class BatchResult(Generic[T_ID]):
    successful_items: list[SuccessItem[T_ID]] = field(default_factory=list)
    failed_items: list[FailedItem[T_ID]] = field(default_factory=list)


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
class WorkItem(Generic[T_ID]):
    items: list[dict]
    attempt: int = 1


class HTTPBatchProcessor(Generic[T_ID]):
    """
    A more performant and readable HTTP batch processor.

    This version uses a producer-consumer pattern with a single work queue,
    and consolidates all logic into a single class for improved readability and reduced code.
    """

    def __init__(
        self,
        endpoint_url: str,
        config: ToolkitClientConfig,
        as_id: Callable[[dict], T_ID],
        method: Literal["POST", "GET"] = "POST",
        batch_size: int = 1_000,
        max_workers: int = 8,
        max_retries: int = 10,
        console: Console | None = None,
        description: str = "Processing items",
    ):
        self.endpoint_url = endpoint_url
        self.method = method.upper()
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
        self._token_lock = threading.Lock()
        self._token: str | None = None
        self._token_expiry = 0.0

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

    def _get_auth_header(self) -> dict[str, str]:
        with self._token_lock:
            if time.time() > self._token_expiry - 60:  # Refresh 1 min before expiry
                auth_header_name, auth_header_value = self._config.credentials.authorization_header()
                self._token_expiry = time.time() + 3600  # Assume 1-hour expiry
            return {auth_header_name: auth_header_value}

    def _handle_rate_limit(self, response: requests.Response) -> None:
        with self._rate_limit_lock:
            retry_after = float(response.headers.get("Retry-After", "1.0"))
            backoff = min(retry_after + random.uniform(0, 1), 60)
            self.console.print(f"[yellow]Rate limit hit (429). Backing off for {backoff:.2f}s.[/yellow]")
            self._rate_limit_until = time.time() + backoff

    def _producer(self, items_iterator: Iterable[T_ID], work_queue: Queue):
        batch = []
        for item in items_iterator:
            batch.append(item)
            if len(batch) >= self.batch_size:
                work_queue.put(WorkItem(items=batch))
                batch = []
        if batch:
            work_queue.put(WorkItem(items=batch))

    def process(self, items: Iterable[dict]) -> ProcessorResult[T_ID]:
        work_queue: Queue[WorkItem[dict] | None] = Queue(maxsize=self.max_workers * 2)
        results_queue: Queue[BatchResult[T_ID]] = Queue()

        producer_thread = threading.Thread(target=self._producer, args=(items, work_queue), daemon=True)
        producer_thread.start()
        batch_results: list[BatchResult[T_ID]] = []
        with Progress(
            TextColumn("[bold blue]{task.description}"), BarColumn(),
            TaskProgressColumn(), TextColumn("{task.fields[processed_items]} items processed"),
            TimeRemainingColumn(), console=self.console
        ) as progress:
            task = progress.add_task(self.description, total=None, processed_items=0)

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
                        processed_count += len(result.successful_items) + len(result.failed_items)
                        progress.update(task, processed_items=processed_count)
                        results_queue.task_done()
                    time.sleep(0.1)  # Prevent busy-waiting

                # Wait for all tasks in the queue (including retries/splits) to be processed.
                work_queue.join()

                # Final drain of the results queue
                while not results_queue.empty():
                    result = results_queue.get()
                    batch_results.append(result)
                    processed_count += len(result.successful_items) + len(result.failed_items)
                    progress.update(task, processed_items=processed_count)
                    results_queue.task_done()

                # Shut down workers gracefully
                for _ in range(self.max_workers):
                    work_queue.put(None)

        return self._aggregate_results(batch_results)

    def _worker(self, work_queue: Queue, results_queue: Queue,):
        while (work_item := work_queue.get()) is not None:
            try:
                # Check for rate limit backoff
                with self._rate_limit_lock:
                    backoff_time = self._rate_limit_until - time.time()
                if backoff_time > 0:
                    time.sleep(backoff_time)

                response = self._make_request(work_item)
                self._handle_response(response, work_item, work_queue, results_queue)

            except requests.RequestException as e:
                self._handle_network_error(e, work_item, work_queue, results_queue)
            finally:
                work_queue.task_done()

    def _make_request(self, work_item: WorkItem[T_ID]) -> requests.Response:
        headers = self._get_auth_header()
        headers.update({"Content-Type": "application/json"})
        payload = {"items": work_item.items}
        return self.session.request(self.method, self.endpoint_url, json=payload, headers=headers, timeout=60)

    def _handle_response(self, response: requests.Response, work_item: WorkItem, work_queue: Queue,
                         results_queue: Queue):
        if 200 <= response.status_code < 300:
            results_queue.put(BatchResult(
                successful_items=[
                    SuccessItem(item=self.as_id(item), operation="create")
                    for item in work_item.items
                ]
            )
            )
        elif response.status_code == 400 and len(work_item.items) > 1:
            # Split and re-queue
            mid = len(work_item.items) // 2
            work_queue.put(WorkItem(items=work_item.items[:mid]))
            work_queue.put(WorkItem(items=work_item.items[mid:]))
        elif response.status_code == 429:
            # Re-queue for rate limiting
            self._handle_rate_limit(response.headers)
            work_queue.put(work_item)
        elif work_item.attempt < self.max_retries and response.status_code >= 500:
            # Retry server errors
            work_item.attempt += 1
            time.sleep((2 ** work_item.attempt) + random.uniform(0, 1))
            work_queue.put(work_item)
        else:
            # Permanent failure
            error_msg = f"HTTP {response.status_code}: {response.text[:200]}"
            failed = [(item, response.status_code, error_msg) for item in work_item.items]
            results_queue.put(BatchResult(failed_items=failed))

    def _handle_network_error(self, e: requests.RequestException, work_item: WorkItem, work_queue: Queue,
                              results_queue: Queue):
        if work_item.attempt < self.max_retries:
            work_item.attempt += 1
            time.sleep((2 ** work_item.attempt) + random.uniform(0, 1))
            work_queue.put(work_item)
        else:
            error_msg = f"RequestException after {self.max_retries} attempts: {str(e)}"
            failed = [(item, 0, error_msg) for item in work_item.items]
            results_queue.put(BatchResult(failed_items=failed))

    def _handle_rate_limit(self, headers: dict) -> None:
        with self._rate_limit_lock:
            retry_after = float(headers.get("Retry-After", "1.0"))
            backoff = min(retry_after + random.uniform(0, 1), 60)
            self.console.print(f"[yellow]Rate limit hit. Backing off for {backoff:.2f}s.[/yellow]")
            self._rate_limit_until = time.time() + backoff

    @staticmethod
    def _aggregate_results(results: list[BatchResult[T_ID]]) -> ProcessorResult[T_ID]:
        successful_items = [item for r in results for item in r.successful_items]
        failed_items = [item for r in results for item in r.failed_items]
        error_summary = Counter()
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
        if hasattr(self, 'session'):
            self.session.close()
