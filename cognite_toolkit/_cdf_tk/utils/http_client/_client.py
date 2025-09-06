import gzip
import random
import socket
import sys
import time
from collections.abc import MutableMapping
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

from ._data_classes import (
    BodyRequestMessage,
    HTTPMessage,
    ItemsRequestMessage,
    RequestMessage,
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

    """

    def __init__(
        self,
        config: ToolkitClientConfig,
        max_retries: int = 10,
        pool_connections: int = 10,
        pool_maxsize: int = 20,
    ):
        self._config = config
        self._max_retries = max_retries
        self._pool_connections = pool_connections
        self._pool_maxsize = pool_maxsize

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
            pool_connections=self._pool_connections,
            pool_maxsize=self._pool_maxsize,
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

    @staticmethod
    def _prepare_payload(item: BodyRequestMessage) -> str | bytes:
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

    def _process_request(self, message: RequestMessage) -> list[HTTPMessage]:
        try:
            response = self._make_request(message)
            results = self._handle_response(response, message)
        except Exception as e:
            results = self._handle_error(e, message)
        return results

    def _make_request(self, item: RequestMessage) -> requests.Response:
        headers = self._create_headers()
        data: str | bytes | None = None
        if isinstance(item, BodyRequestMessage):
            data = self._prepare_payload(item)
        return self.session.request(
            method=item.method,
            url=item.endpoint_url,
            data=data,
            headers=headers,
            timeout=self._config.timeout,
            allow_redirects=False,
        )

    def _handle_response(
        self,
        response: requests.Response,
        request: RequestMessage,
    ) -> list[HTTPMessage]:
        if 200 <= response.status_code < 300:
            return request.create_success(response)
        elif response.status_code in {401, 403}:
            error_msg = f"Authentication error (status code {response.status_code}): check your API key and project"
            return self._create_failed_responses(response, request, error_msg)
        elif response.status_code in {400, 408, 409, 422, 502, 503, 504} and isinstance(request, ItemsRequestMessage):
            # 400, 409, 422: There is at least one item that is invalid, split the batch to get all valid items processed
            # 502, 503, 504: Server errors, retry in smaller batches, count as a status_attempt.
            # 408: Request timeout, retry in smaller batches
            status_attempts = request.status_attempt
            if response.status_code in {502, 503, 504}:
                status_attempts += 1
            return request.split(status_attempts=status_attempts)
        elif request.status_attempt < self._max_retries and response.status_code in {408, 429, 502, 503, 504}:
            request.status_attempt += 1
            time.sleep(self._backoff_time(request.total_attempts))
            return [request]
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
            return self._create_failed_responses(response, request, error)

    @staticmethod
    def _backoff_time(attempts: int) -> float:
        backoff_time = 0.5 * (2**attempts)
        return min(backoff_time, global_config.max_retry_backoff) * random.uniform(0, 1.0)

    def _handle_error(
        self,
        e: Exception,
        request: RequestMessage,
    ) -> list[HTTPMessage]:
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
            return self._create_failed_requests(error_msg, request)

        if attempts < self._max_retries:
            time.sleep(self._backoff_time(request.total_attempts))
            return [request]
        else:
            error_msg = f"RequestException after {self._max_retries} {error_type} attempts: {e!s}"

            return self._create_failed_requests(error_msg, request)

    def _create_failed_responses(
        self,
        response: requests.Response,
        request: RequestMessage,
        error_message: str,
    ) -> list[HTTPMessage]:
        raise NotImplementedError()

    def _create_failed_requests(
        self,
        error_message: str,
        request: RequestMessage,
    ) -> list[HTTPMessage]:
        raise NotImplementedError()

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
