from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor

from rich import Console

from cognite_toolkit._cdf_tk.utils.http_client import (
    HTTPClient,
    HTTPResult2,
    ItemsRequest2,
    ItemsResultMessage2,
    RequestMessage2,
)


class CDFClient:
    def __init__(self, http_client: HTTPClient, console: Console, max_workers: int = 8) -> None:
        self.http_client = http_client
        self.console = console
        self._config = http_client.config
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
        results: list[HTTPResult2] = []
        raise NotImplementedError()

    def request_items_concurrent(
        self, requests: Sequence[ItemsRequest2], max_concurrent: int | None = None
    ) -> list[ItemsResultMessage2]:
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
        raise NotImplementedError()
