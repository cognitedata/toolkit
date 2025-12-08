import gzip
import random
import sys
import time
from collections import deque
from collections.abc import MutableMapping, Sequence, Set
from typing import Literal, TypeVar

import httpx
from cognite.client import global_config
from rich.console import Console

from cognite_toolkit._cdf_tk.tk_warnings import HighSeverityWarning
from cognite_toolkit._cdf_tk.utils.auxiliary import get_current_toolkit_version, get_user_agent
from cognite_toolkit._cdf_tk.utils.http_client._data_classes import (
    BodyRequest,
    DataBodyRequest,
    FailedRequestMessage,
    HTTPMessage,
    ItemsRequest,
    ParamRequest,
    RequestMessage,
    ResponseList,
    ResponseMessage,
)
from cognite_toolkit._cdf_tk.utils.http_client._data_classes2 import (
    BaseRequestMessage,
    ErrorDetails2,
    FailedRequest2,
    FailedResponse2,
    HTTPResult2,
    ItemsFailedRequest2,
    ItemsFailedResponse2,
    ItemsRequest2,
    ItemsResultList,
    ItemsResultMessage2,
    ItemsSuccessResponse2,
    RequestMessage2,
    SuccessResponse2,
)
from cognite_toolkit._cdf_tk.utils.useful_types import PrimitiveType

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

from cognite_toolkit._cdf_tk.client.config import ToolkitClientConfig

_T_Request_Message = TypeVar("_T_Request_Message", bound=BaseRequestMessage)


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
        console (Console | None): Optional Rich Console for printing warnings.

    """

    def __init__(
        self,
        config: ToolkitClientConfig,
        max_retries: int = 10,
        pool_connections: int = 10,
        pool_maxsize: int = 20,
        retry_status_codes: Set[int] = frozenset({408, 429, 502, 503, 504}),
        split_items_status_codes: Set[int] = frozenset({400, 408, 409, 422, 502, 503, 504}),
        console: Console | None = None,
    ):
        self.config = config
        self._max_retries = max_retries
        self._pool_connections = pool_connections
        self._pool_maxsize = pool_maxsize
        self._retry_status_codes = retry_status_codes
        self._split_items_status_codes = split_items_status_codes
        self._console = console

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
        if isinstance(message, ItemsRequest) and message.tracker and message.tracker.limit_reached():
            error_msg = (
                f"Aborting further splitting of requests after {message.tracker.failed_split_count} failed attempts."
            )
            return message.create_failed_request(error_msg)
        try:
            response = self._make_request(message)
            results = self._handle_response(response, message)
        except Exception as e:
            results = self._handle_error(e, message)
        return results

    def request_with_retries(self, message: RequestMessage) -> ResponseList:
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
        final_responses = ResponseList([])
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

    def _create_thread_safe_session(self) -> httpx.Client:
        return httpx.Client(
            limits=httpx.Limits(
                max_connections=self._pool_maxsize,
                max_keepalive_connections=self._pool_connections,
            ),
            timeout=self.config.timeout,
        )

    def _create_headers(
        self,
        api_version: str | None = None,
        content_type: str = "application/json",
        accept: str = "application/json",
        content_length: int | None = None,
    ) -> MutableMapping[str, str]:
        headers: MutableMapping[str, str] = {}
        headers["User-Agent"] = f"httpx/{httpx.__version__} {get_user_agent()}"
        auth_name, auth_value = self.config.credentials.authorization_header()
        headers[auth_name] = auth_value
        headers["Content-Type"] = content_type
        if content_length is not None:
            headers["Content-Length"] = str(content_length)
        headers["accept"] = accept
        headers["x-cdp-sdk"] = f"CogniteToolkit:{get_current_toolkit_version()}"
        headers["x-cdp-app"] = self.config.client_name
        headers["cdf-version"] = api_version or self.config.api_subversion
        if not global_config.disable_gzip and content_length is None:
            headers["Content-Encoding"] = "gzip"
        return headers

    def _make_request(self, item: RequestMessage) -> httpx.Response:
        headers = self._create_headers(item.api_version, item.content_type, item.accept, item.content_length)
        params: dict[str, PrimitiveType] | None = None
        if isinstance(item, ParamRequest):
            params = item.parameters
        data: str | bytes | None = None
        if isinstance(item, BodyRequest):
            data = item.data()
            if not global_config.disable_gzip and item.content_length is None:
                data = gzip.compress(data.encode("utf-8"))
        elif isinstance(item, DataBodyRequest):
            data = item.data()
            if not global_config.disable_gzip and item.content_length is None:
                data = gzip.compress(data)
        return self.session.request(
            method=item.method,
            url=item.endpoint_url,
            content=data,
            headers=headers,
            params=params,
            timeout=self.config.timeout,
            follow_redirects=False,
        )

    def _handle_response(self, response: httpx.Response, request: RequestMessage) -> Sequence[HTTPMessage]:
        if 200 <= response.status_code < 300:
            return request.create_success_response(response)
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
            splits = request.split(status_attempts=status_attempts)
            if splits[0].tracker and splits[0].tracker.limit_reached():
                return request.create_failure_response(response)
            return splits

        retry_after = self._get_retry_after_in_header(response)
        if retry_after is not None and response.status_code == 429 and request.status_attempt < self._max_retries:
            if self._console is not None:
                short_url = request.endpoint_url.removeprefix(self.config.base_api_url)
                HighSeverityWarning(
                    f"Rate limit exceeded for the {short_url!r} endpoint. Retrying after {retry_after} seconds."
                ).print_warning(console=self._console)
            request.status_attempt += 1
            time.sleep(retry_after)
            return [request]

        if request.status_attempt < self._max_retries and response.status_code in self._retry_status_codes:
            request.status_attempt += 1
            time.sleep(self._backoff_time(request.total_attempts))
            return [request]
        else:
            # Permanent failure
            return request.create_failure_response(response)

    @staticmethod
    def _get_retry_after_in_header(response: httpx.Response) -> float | None:
        if "Retry-After" not in response.headers:
            return None
        try:
            return float(response.headers["Retry-After"])
        except ValueError:
            # Ignore invalid Retry-After header
            return None

    @staticmethod
    def _backoff_time(attempts: int) -> float:
        backoff_time = 0.5 * (2**attempts)
        return min(backoff_time, global_config.max_retry_backoff) * random.uniform(0, 1.0)

    def _handle_error(
        self,
        e: Exception,
        request: RequestMessage,
    ) -> Sequence[HTTPMessage]:
        if isinstance(e, httpx.ReadTimeout | httpx.TimeoutException):
            error_type = "read"
            request.read_attempt += 1
            attempts = request.read_attempt
        elif isinstance(e, ConnectionError | httpx.ConnectError | httpx.ConnectTimeout):
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

    def request_single(self, message: RequestMessage2) -> RequestMessage2 | HTTPResult2:
        """Send an HTTP request and return the response.

        Args:
            message (RequestMessage2): The request message to send.
        Returns:
            HTTPMessage: The response message.
        """
        try:
            response = self._make_request2(message)
            result = self._handle_response_single(response, message)
        except Exception as e:
            result = self._handle_error_single(e, message)
        return result

    def request_single_retries(self, message: RequestMessage2) -> HTTPResult2:
        """Send an HTTP request and handle retries.

        This method will keep retrying the request until it either succeeds or
        exhausts the maximum number of retries.

        Note this method will use the current thread to process all request, thus
        it is blocking.

        Args:
            message (RequestMessage2): The request message to send.
        Returns:
            HTTPMessage2: The final response message, which can be either successful response or failed request.
        """
        if message.total_attempts > 0:
            raise RuntimeError(f"RequestMessage has already been attempted {message.total_attempts} times.")
        current_request = message
        while True:
            result = self.request_single(current_request)
            if isinstance(result, RequestMessage2):
                current_request = result
            elif isinstance(result, HTTPResult2):
                return result
            else:
                raise TypeError(f"Unexpected result type: {type(result)}")

    def _make_request2(self, message: BaseRequestMessage) -> httpx.Response:
        headers = self._create_headers(message.api_version, message.content_type, message.accept)
        return self.session.request(
            method=message.method,
            url=message.endpoint_url,
            content=message.content,
            headers=headers,
            params=message.parameters,
            timeout=self.config.timeout,
            follow_redirects=False,
        )

    def _handle_response_single(
        self, response: httpx.Response, request: RequestMessage2
    ) -> RequestMessage2 | HTTPResult2:
        if 200 <= response.status_code < 300:
            return SuccessResponse2(
                status_code=response.status_code,
                body=response.text,
                content=response.content,
            )
        if retry_request := self._retry_request(response, request):
            return retry_request
        else:
            # Permanent failure
            return FailedResponse2(
                status_code=response.status_code,
                body=response.text,
                error=ErrorDetails2.from_response(response),
            )

    def _retry_request(self, response: httpx.Response, request: _T_Request_Message) -> _T_Request_Message | None:
        retry_after = self._get_retry_after_in_header(response)
        if retry_after is not None and response.status_code == 429 and request.status_attempt < self._max_retries:
            if self._console is not None:
                short_url = request.endpoint_url.removeprefix(self.config.base_api_url)
                HighSeverityWarning(
                    f"Rate limit exceeded for the {short_url!r} endpoint. Retrying after {retry_after} seconds."
                ).print_warning(console=self._console)
            request.status_attempt += 1
            time.sleep(retry_after)
            return request

        if request.status_attempt < self._max_retries and response.status_code in self._retry_status_codes:
            request.status_attempt += 1
            time.sleep(self._backoff_time(request.total_attempts))
            return request
        return None

    def _handle_error_single(self, e: Exception, request: RequestMessage2) -> RequestMessage2 | HTTPResult2:
        if isinstance(e, httpx.ReadTimeout | httpx.TimeoutException):
            error_type = "read"
            request.read_attempt += 1
            attempts = request.read_attempt
        elif isinstance(e, ConnectionError | httpx.ConnectError | httpx.ConnectTimeout):
            error_type = "connect"
            request.connect_attempt += 1
            attempts = request.connect_attempt
        else:
            error_msg = f"Unexpected exception: {e!s}"
            return FailedRequest2(error=error_msg)

        if attempts <= self._max_retries:
            time.sleep(self._backoff_time(request.total_attempts))
            return request
        else:
            # We have already incremented the attempt count, so we subtract 1 here
            error_msg = f"RequestException after {request.total_attempts - 1} attempts ({error_type} error): {e!s}"

            return FailedRequest2(error=error_msg)

    def request_items(self, message: ItemsRequest2) -> Sequence[ItemsRequest2 | ItemsResultMessage2]:
        """Send an HTTP request with multiple items and return the response.

        Args:
            message (ItemsRequest2): The request message to send.
        Returns:
            Sequence[ItemsRequest2 | ItemsResultMessage2]: The response message(s). This can also
                include ItemsRequest2(s) to be retried or split.
        """
        if message.tracker and message.tracker.limit_reached():
            return [
                ItemsFailedRequest2(
                    ids=[item.as_id() for item in message.items],
                    error_message=f"Aborting further splitting of requests after {message.tracker.failed_split_count} failed attempts.",
                )
            ]
        try:
            response = self._make_request2(message)
            results = self._handle_items_response(response, message)
        except Exception as e:
            results = self._handle_items_error(e, message)
        return results

    def request_items_retries(self, message: ItemsRequest2) -> ItemsResultList:
        """Send an HTTP request with multiple items and handle retries.

        This method will keep retrying the request until it either succeeds or
        exhausts the maximum number of retries.

        Note this method will use the current thread to process all request, thus
        it is blocking.

        Args:
            message (ItemsRequest2): The request message to send.
        Returns:
            Sequence[ItemsResultMessage2]: The final response message, which can be either successful response or failed request.
        """
        if message.total_attempts > 0:
            raise RuntimeError(f"ItemsRequest2 has already been attempted {message.total_attempts} times.")
        pending_requests: deque[ItemsRequest2] = deque()
        pending_requests.append(message)
        final_responses = ItemsResultList([])
        while pending_requests:
            current_request = pending_requests.popleft()
            results = self.request_items(current_request)

            for result in results:
                if isinstance(result, ItemsRequest2):
                    pending_requests.append(result)
                elif isinstance(result, ItemsResultMessage2):
                    final_responses.append(result)
                else:
                    raise TypeError(f"Unexpected result type: {type(result)}")

        return final_responses

    def _handle_items_response(
        self, response: httpx.Response, request: ItemsRequest2
    ) -> Sequence[ItemsRequest2 | ItemsResultMessage2]:
        if 200 <= response.status_code < 300:
            return [
                ItemsSuccessResponse2(
                    ids=[item.as_id() for item in request.items],
                    status_code=response.status_code,
                    body=response.text,
                    content=response.content,
                )
            ]
        elif len(request.items) > 1 and response.status_code in self._split_items_status_codes:
            # 4XX: Status there is at least one item that is invalid, split the batch to get all valid items processed
            # 5xx: Server error, split to reduce the number of items in each request, and count as a status attempt
            status_attempts = request.status_attempt
            if 500 <= response.status_code < 600:
                status_attempts += 1
            splits = request.split(status_attempts=status_attempts)
            if splits[0].tracker and splits[0].tracker.limit_reached():
                return [
                    ItemsFailedResponse2(
                        ids=[item.as_id() for item in request.items],
                        status_code=response.status_code,
                        body=response.text,
                        error=ErrorDetails2.from_response(response),
                    )
                ]
            return splits

        if retry_request := self._retry_request(response, request):
            return [retry_request]
        else:
            # Permanent failure
            return [
                ItemsFailedResponse2(
                    ids=[item.as_id() for item in request.items],
                    status_code=response.status_code,
                    body=response.text,
                    error=ErrorDetails2.from_response(response),
                )
            ]

    def _handle_items_error(
        self, e: Exception, request: ItemsRequest2
    ) -> Sequence[ItemsRequest2 | ItemsResultMessage2]:
        if isinstance(e, httpx.ReadTimeout | httpx.TimeoutException):
            error_type = "read"
            request.read_attempt += 1
            attempts = request.read_attempt
        elif isinstance(e, ConnectionError | httpx.ConnectError | httpx.ConnectTimeout):
            error_type = "connect"
            request.connect_attempt += 1
            attempts = request.connect_attempt
        else:
            error_msg = f"Unexpected exception: {e!s}"
            return [
                ItemsFailedRequest2(
                    ids=[item.as_id() for item in request.items],
                    error_message=error_msg,
                )
            ]

        if attempts <= self._max_retries:
            time.sleep(self._backoff_time(request.total_attempts))
            return [request]
        else:
            # We have already incremented the attempt count, so we subtract 1 here
            error_msg = f"RequestException after {request.total_attempts - 1} attempts ({error_type} error): {e!s}"

            return [
                ItemsFailedRequest2(
                    ids=[item.as_id() for item in request.items],
                    error_message=error_msg,
                )
            ]
