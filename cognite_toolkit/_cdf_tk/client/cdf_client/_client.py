import time
from collections.abc import Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Literal

from pydantic import JsonValue, TypeAdapter
from rich import Console

from cognite_toolkit._cdf_tk.client.data_classes.base import T_ResponseResource
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence
from cognite_toolkit._cdf_tk.utils.http_client import (
    HTTPClient,
    HTTPResult2,
    ItemsRequest2,
    ItemsResultMessage2,
    RequestMessage2,
    RequestResource,
)
from cognite_toolkit._cdf_tk.utils.http_client._data_classes2 import ItemsResultList


class CDFClient:
    def __init__(self, http_client: HTTPClient, console: Console, max_workers: int = 8) -> None:
        self.http_client = http_client
        self.console = console
        self.config = http_client.config
        self._pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="cdf")

    def _close(self) -> None:
        self._pool.shutdown(wait=True)

    def request_concurrent(
        self, requests: Sequence[RequestMessage2], max_concurrent: int | None = None
    ) -> list[HTTPResult2]:
        """Send multiple requests concurrently.

        Args:
            requests (Sequence[RequestMessage2]): The requests to send.
            max_concurrent (int | None): The maximum number of concurrent requests. If None, uses the default from the thread pool.

        Returns:
            list[HTTPResult2]: The results of the requests.
        """
        if max_concurrent is not None and max_concurrent > self._pool._max_workers:
            raise ValueError(
                f"max_concurrent {max_concurrent} exceeds the thread pool max_workers {self._pool._max_workers}"
            )
        to_request = list(requests)
        futures: list[Future] = []
        results: list[HTTPResult2] = []
        concurrent_count = 0
        while to_request or futures:
            if max_concurrent is None or concurrent_count < max_concurrent:
                item = to_request.pop()
                concurrent_count += 1
                futures.append(self._pool.submit(self.http_client.request_single_retries, item))
            else:
                ongoing_futures: list[Future] = []
                for future in futures:
                    if future.done():
                        results.append(future.result())
                        concurrent_count -= 1
                    else:
                        ongoing_futures.append(future)
                futures = ongoing_futures
                time.sleep(0.5)
        return results

    def request_items_concurrent(
        self, requests: Sequence[ItemsRequest2], max_concurrent: int | None = None
    ) -> ItemsResultList:
        """Send multiple item requests concurrently.

        Args:
            requests (Sequence[ItemsRequest2]): The item requests to send.
            max_concurrent (int | None): The maximum number of concurrent requests. If None, uses the default from the thread pool.

        Returns:
            list[ItemsResultMessage2]: The results of the item requests.
        """
        if max_concurrent is not None and max_concurrent > self._pool._max_workers:
            raise ValueError(
                f"max_concurrent {max_concurrent} exceeds the thread pool max_workers {self._pool._max_workers}"
            )
        to_request = list(requests)
        futures: list[Future] = []
        results = ItemsResultList()
        concurrent_count = 0
        while to_request or futures:
            if to_request and (max_concurrent is None or concurrent_count < max_concurrent):
                item = to_request.pop()
                concurrent_count += 1
                futures.append(self._pool.submit(self.http_client.request_items, item))
            else:
                ongoing_futures: list[Future] = []
                for future in futures:
                    if future.done():
                        result = future.result()
                        for result_item in result:
                            if isinstance(result_item, ItemsRequest2):
                                to_request.append(result_item)
                            elif isinstance(result_item, ItemsResultMessage2):
                                results.append(result_item)
                            else:
                                raise TypeError(f"Unexpected result type: {type(result)}")
                        concurrent_count -= 1
                    else:
                        ongoing_futures.append(future)
                futures = ongoing_futures
                time.sleep(0.5)
        return results

    def request_resource_items(
        self,
        items: Sequence[RequestResource],
        endpoint_url: str,
        method: Literal["GET", "POST", "PATCH", "DELETE", "PUT"],
        request_limit: int,
        response_type: type[T_ResponseResource],
        extra_body_fields: dict[str, JsonValue] | None = None,
        max_concurrent: int | None = None,
    ) -> list[T_ResponseResource]:
        """Send item requests for resources.

        Args:
            items (Sequence[RequestResource]): The resources to send in the requests.
            endpoint_url (str): The endpoint URL for the requests.
            method (str): The HTTP method for the requests.
            request_limit (int): The maximum number of items per request.
            response_type (type[T_ResponseResource]): The type of the response resources.
            max_concurrent (int | None): The maximum number of concurrent requests. If None, uses the default from the thread pool.

        Returns:
            list[T_ResponseResource]: The response resources.
        """
        request_items = [
            ItemsRequest2(endpoint_url=endpoint_url, method=method, items=chunk, extra_body_fields=extra_body_fields)
            for chunk in chunker_sequence(items, request_limit)
        ]
        response = self.request_items_concurrent(request_items, max_concurrent)
        response.raise_for_status()
        return TypeAdapter(list[response_type]).validate_python(response.get_items())
