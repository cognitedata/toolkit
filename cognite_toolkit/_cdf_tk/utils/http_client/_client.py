import gzip
import random
import socket
import sys
import time
from collections import deque
from collections.abc import MutableMapping, Sequence, Set
from typing import Literal

import requests
import urllib3
from cognite.client import global_config
from cognite.client.utils import _json
from requests.adapters import HTTPAdapter
from requests.structures import CaseInsensitiveDict
from urllib3.util.retry import Retry

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.utils.auxiliary import get_current_toolkit_version, get_user_agent
from cognite_toolkit._cdf_tk.utils.http_client._data_classes import (
    BodyRequest,
    FailedRequestMessage,
    HTTPMessage,
    ItemsRequest,
    ParamRequest,
    RequestMessage,
    ResponseMessage,
)

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class HTTPClient:
    """An HTTP client.

    This class handles rate limiting, retries, and error handling for HTTP requests.

    Args:
        config (ToolkitClientConfig): Configuration for the Toolkit client.
        pool_connections (int): The number of connection pools to cache. Default is 10.
        pool_maxsize (int): The maximum number of connections to save in the pool. Default
            is 20.
        max_retries (int): The maximum number of retries for a request. Default is 10.
        retry_status_codes (frozenset[int]): HTTP status codes that should trigger a retry.
            Default is {408, 429, 502, 503, 504}.
        split_items_status_codes (frozenset[int]): In the case of ItemRequest with multiple
            items, these status codes will trigger splitting the request into smaller batches.

    """

    def __init__(
        self,
        config: ToolkitClientConfig,
        max_retries: int = 10,
        pool_connections: int = 10,
        pool_maxsize: int = 20,
        retry_status_codes: Set[int] = frozenset({408, 429, 502, 503, 504}),
        split_items_status_codes: Set[int] = frozenset({400, 408, 409, 422, 502, 503, 504}),
    ):
        self._config = config
        self._max_retries = max_retries
        self._pool_connections = pool_connections
        self._pool_maxsize = pool_maxsize
        self._retry_status_codes = retry_status_codes
        self._split_items_status_codes = split_items_status_codes

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

    def request(self, message: RequestMessage) -> Sequence[HTTPMessage]:
        """Send an HTTP request and return the response.

        Args:
            message (RequestMessage): The request message to send.

        Returns:
            Sequence[HTTPMessage]: The response message(s). This can also
                include RequestMessage(s) to be retried.
        """
        try:
            response = self._make_request(message)
            results = self._handle_response(response, message)
        except Exception as e:
            results = self._handle_error(e, message)
        return results

    def request_with_retries(self, message: RequestMessage) -> Sequence[ResponseMessage | FailedRequestMessage]:
        """Send an HTTP request and handle retries.

        This method will keep retrying the request until it either succeeds or
        exhausts the maximum number of retries.

        Note this method will use the current thread to process all request, thus
        it is blocking.

        Args:
            message (RequestMessage): The request message to send.

        Returns:
            Sequence[ResponseMessage | FailedRequestMessage]: The final response
                messages, which can be either successful responses or failed requests.
        """
        if message.total_attempts > 0:
            raise RuntimeError(f"RequestMessage has already been attempted {message.total_attempts} times.")
        pending_requests: deque[RequestMessage] = deque()
        pending_requests.append(message)
        final_responses: list[ResponseMessage | FailedRequestMessage] = []

        while pending_requests:
            current_request = pending_requests.popleft()
            results = self.request(current_request)

            for result in results:
                if isinstance(result, RequestMessage):
                    pending_requests.append(result)
                elif isinstance(result, ResponseMessage | FailedRequestMessage):
                    final_responses.append(result)
                else:
                    raise TypeError(f"Unexpected result type: {type(result)}")

        return final_responses

    def _create_thread_safe_session(self) -> requests.Session:
        session = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=self._pool_connections,
            pool_maxsize=self._pool_maxsize,
            max_retries=Retry(total=0),  # We handle retries manually
        )
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

    @staticmethod
    def _prepare_payload(item: BodyRequest) -> str | bytes:
        """
        Prepare the payload for the HTTP request.
        This method should be overridden in subclasses to customize the payload format.
        """
        data: str | bytes
        try:
            data = _json.dumps(item.body(), allow_nan=False)
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

    def _make_request(self, item: RequestMessage) -> requests.Response:
        headers = self._create_headers()
        params: dict[str, str] | None = None
        if isinstance(item, ParamRequest):
            params = item.parameters
        data: str | bytes | None = None
        if isinstance(item, BodyRequest):
            data = self._prepare_payload(item)
        return self.session.request(
            method=item.method,
            url=item.endpoint_url,
            data=data,
            headers=headers,
            params=params,
            timeout=self._config.timeout,
            allow_redirects=False,
        )

    def _handle_response(
        self,
        response: requests.Response,
        request: RequestMessage,
    ) -> Sequence[HTTPMessage]:
        try:
            body = response.json()
        except ValueError as e:
            return request.create_responses(response, error_message=f"Invalid JSON response: {e!s}")

        if 200 <= response.status_code < 300:
            return request.create_responses(response, body)
        elif (
            isinstance(request, ItemsRequest)
            and len(request.items) > 1
            and response.status_code in self._split_items_status_codes
        ):
            # 4XX: Status there is at least one item that is invalid, split the batch to get all valid items processed
            # 5xx: Server error, split to reduce the number of items in each request, and count as a status attempt
            status_attempts = request.status_attempt
            if 500 <= response.status_code < 600:
                status_attempts += 1
            return request.split(status_attempts=status_attempts)
        elif request.status_attempt < self._max_retries and response.status_code in self._retry_status_codes:
            request.status_attempt += 1
            time.sleep(self._backoff_time(request.total_attempts))
            return [request]
        else:
            # Permanent failure
            error = response.text
            if "error" in body:
                if isinstance(body["error"], str):
                    error = body["error"]
                elif isinstance(body["error"], dict) and "message" in body["error"]:
                    error = body["error"]["message"]
            return request.create_responses(response, body, error)

    @staticmethod
    def _backoff_time(attempts: int) -> float:
        backoff_time = 0.5 * (2**attempts)
        return min(backoff_time, global_config.max_retry_backoff) * random.uniform(0, 1.0)

    def _handle_error(
        self,
        e: Exception,
        request: RequestMessage,
    ) -> Sequence[HTTPMessage]:
        if self._any_exception_in_context_isinstance(
            e, (socket.timeout, urllib3.exceptions.ReadTimeoutError, requests.exceptions.ReadTimeout)
        ):
            error_type = "read"
            request.read_attempt += 1
            attempts = request.read_attempt
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
            request.connect_attempt += 1
            attempts = request.connect_attempt
        else:
            error_msg = f"Unexpected exception: {e!s}"
            return request.create_failed_request(error_msg)

        if attempts <= self._max_retries:
            time.sleep(self._backoff_time(request.total_attempts))
            return [request]
        else:
            # We have already incremented the attempt count, so we subtract 1 here
            error_msg = f"RequestException after {request.total_attempts - 1} attempts ({error_type} error): {e!s}"

            return request.create_failed_request(error_msg)

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
