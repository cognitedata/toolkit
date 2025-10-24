from abc import ABC, abstractmethod
from collections import UserList
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Generic, Literal, Protocol, TypeAlias

import httpx
from cognite.client.utils import _json

from cognite_toolkit._cdf_tk.utils.http_client._exception import ToolkitAPIError
from cognite_toolkit._cdf_tk.utils.http_client._tracker import ItemsRequestTracker
from cognite_toolkit._cdf_tk.utils.useful_types import T_ID, JsonVal, PrimitiveType

StatusCode: TypeAlias = int


@dataclass
class HTTPMessage(ABC):
    """Base class for HTTP messages (requests and responses)"""

    def dump(self) -> dict[str, JsonVal]:
        """Dumps the message to a JSON serializable dictionary.

        Returns:
            dict[str, JsonVal]: The message as a dictionary.
        """
        # We avoid using the asdict function as we know we have a shallow structure,
        # and this roughly ~10x faster.
        output = self.__dict__.copy()
        output["type"] = type(self).__name__
        return output


@dataclass
class ErrorDetails:
    """This is the expected structure of error details in the CDF API"""

    code: int
    message: str
    missing: list[JsonVal] | None = None
    duplicated: list[JsonVal] | None = None
    is_auto_retryable: bool | None = None

    @classmethod
    def from_response(cls, response: httpx.Response) -> "ErrorDetails":
        try:
            error_data = response.json()["error"]
            if not isinstance(error_data, dict):
                return cls(code=response.status_code, message=str(error_data))
            return cls(
                code=error_data["code"],
                message=error_data["message"],
                missing=error_data.get("missing"),
                duplicated=error_data.get("duplicated"),
                is_auto_retryable=error_data.get("isAutoRetryable"),
            )
        except (ValueError, KeyError):
            # Fallback if response is not JSON or does not have expected structure
            return cls(code=response.status_code, message=response.text)

    def dump(self) -> dict[str, JsonVal]:
        output: dict[str, JsonVal] = {
            "code": self.code,
            "message": self.message,
        }
        if self.missing is not None:
            output["missing"] = self.missing
        if self.duplicated is not None:
            output["duplicated"] = self.duplicated
        if self.is_auto_retryable is not None:
            output["isAutoRetryable"] = self.is_auto_retryable
        return output


@dataclass
class FailedRequestMessage(HTTPMessage):
    error: str


@dataclass
class ResponseMessage(HTTPMessage):
    status_code: StatusCode


@dataclass
class RequestMessage(HTTPMessage):
    """Base class for HTTP request messages"""

    endpoint_url: str
    method: Literal["GET", "POST", "PATCH", "DELETE"]
    connect_attempt: int = 0
    read_attempt: int = 0
    status_attempt: int = 0
    api_version: str | None = None

    @property
    def total_attempts(self) -> int:
        return self.connect_attempt + self.read_attempt + self.status_attempt

    @abstractmethod
    def create_success_response(self, response: httpx.Response) -> Sequence[HTTPMessage]:
        raise NotImplementedError()

    @abstractmethod
    def create_failure_response(self, response: httpx.Response) -> Sequence[HTTPMessage]:
        raise NotImplementedError()

    @abstractmethod
    def create_failed_request(self, error_message: str) -> Sequence[HTTPMessage]:
        raise NotImplementedError()


@dataclass
class SuccessResponse(ResponseMessage):
    body: str


@dataclass
class FailedResponse(ResponseMessage):
    body: str
    error: ErrorDetails

    def dump(self) -> dict[str, JsonVal]:
        output = super().dump()
        output["error"] = self.error.dump()
        return output


@dataclass
class SimpleRequest(RequestMessage):
    """Base class for requests with a simple success/fail response structure"""

    @classmethod
    def create_success_response(cls, response: httpx.Response) -> Sequence[ResponseMessage]:
        return [SuccessResponse(status_code=response.status_code, body=response.text)]

    @classmethod
    def create_failure_response(cls, response: httpx.Response) -> Sequence[HTTPMessage]:
        return [
            FailedResponse(
                status_code=response.status_code, error=ErrorDetails.from_response(response), body=response.text
            )
        ]

    @classmethod
    def create_failed_request(cls, error_message: str) -> Sequence[HTTPMessage]:
        return [FailedRequestMessage(error=error_message)]


@dataclass
class BodyRequest(RequestMessage, ABC):
    """Base class for HTTP request messages with a body"""

    @abstractmethod
    def data(self) -> str:
        raise NotImplementedError()


@dataclass
class ParamRequest(SimpleRequest):
    """Base class for HTTP request messages with query parameters"""

    parameters: dict[str, PrimitiveType] | None = None


@dataclass
class SimpleBodyRequest(SimpleRequest, BodyRequest):
    body_content: dict[str, JsonVal] = field(default_factory=dict)

    def data(self) -> str:
        return _dump_body(self.body_content)


@dataclass
class ItemMessage(Generic[T_ID], ABC):
    """Base class for message related to a specific item identified by an ID"""

    ids: list[T_ID] = field(default_factory=list)


@dataclass
class SuccessResponseItems(ItemMessage[T_ID], SuccessResponse): ...


@dataclass
class FailedResponseItems(ItemMessage[T_ID], FailedResponse): ...


@dataclass
class FailedRequestItems(ItemMessage[T_ID], FailedRequestMessage): ...


class RequestItem(Generic[T_ID], Protocol):
    def dump(self) -> JsonVal: ...

    def as_id(self) -> T_ID: ...


@dataclass
class ItemsRequest(Generic[T_ID], BodyRequest):
    """Requests message for endpoints that accept multiple items in a single request.

    This class provides functionality to split large requests into smaller ones, handle responses for each item,
    and manage errors effectively.

    Attributes:
        items (list[T_RequestItem]): The list of items to be sent in the request body.
        extra_body_fields (dict[str, JsonVal]): Additional fields to include in the request body
        max_failures_before_abort (int): The maximum number of failed split requests before aborting further splits.

    """

    items: list[RequestItem[T_ID]] = field(default_factory=list)
    extra_body_fields: dict[str, JsonVal] = field(default_factory=dict)
    max_failures_before_abort: int = 50
    tracker: ItemsRequestTracker | None = field(default=None, init=False)

    def dump(self) -> dict[str, JsonVal]:
        """Dumps the message to a JSON serializable dictionary.

        This override removes the 'as_id' attribute as it is not serializable.

        Returns:
            dict[str, JsonVal]: The message as a dictionary.
        """
        output = super().dump()
        output["items"] = self.dump_items()
        if self.tracker is not None:
            # We cannot serialize the tracker
            del output["tracker"]
        return output

    def dump_items(self) -> list[JsonVal]:
        """Dumps the items to a list of JSON serializable dictionaries.

        Returns:
            list[JsonVal]: The items as a list of dictionaries.
        """
        return [item.dump() for item in self.items]

    def body(self) -> dict[str, JsonVal]:
        if self.extra_body_fields:
            return {"items": self.dump_items(), **self.extra_body_fields}
        return {"items": self.dump_items()}

    def data(self) -> str:
        return _dump_body(self.body())

    def split(self, status_attempts: int) -> "list[ItemsRequest]":
        """Splits the request into two smaller requests.

        This is useful for retrying requests that fail due to size limits or timeouts.

        Args:
            status_attempts: The number of status attempts to set for the new requests. This is used when the
                request failed with a 5xx status code and we want to track the number of attempts. For 4xx errors,
                there is at least one item causing the error, so we do not increment the status attempts, but
                instead essentially do a binary search to find the problematic item(s).

        Returns:
            A list containing two new ItemsRequest instances, each with half of the original items.

        """
        mid = len(self.items) // 2
        if mid == 0:
            return [self]
        tracker = self.tracker or ItemsRequestTracker(self.max_failures_before_abort)
        tracker.register_failure()
        first_half = ItemsRequest[T_ID](
            endpoint_url=self.endpoint_url,
            method=self.method,
            items=self.items[:mid],
            extra_body_fields=self.extra_body_fields,
            connect_attempt=self.connect_attempt,
            read_attempt=self.read_attempt,
            status_attempt=status_attempts,
        )
        first_half.tracker = tracker
        second_half = ItemsRequest[T_ID](
            endpoint_url=self.endpoint_url,
            method=self.method,
            items=self.items[mid:],
            extra_body_fields=self.extra_body_fields,
            connect_attempt=self.connect_attempt,
            read_attempt=self.read_attempt,
            status_attempt=status_attempts,
        )
        second_half.tracker = tracker
        return [first_half, second_half]

    def create_success_response(self, response: httpx.Response) -> Sequence[HTTPMessage]:
        ids = [item.as_id() for item in self.items]
        return [SuccessResponseItems(status_code=response.status_code, ids=ids, body=response.text)]

    def create_failure_response(self, response: httpx.Response) -> Sequence[HTTPMessage]:
        error = ErrorDetails.from_response(response)
        ids = [item.as_id() for item in self.items]
        return [FailedResponseItems(status_code=response.status_code, ids=ids, error=error, body=response.text)]

    def create_failed_request(self, error_message: str) -> Sequence[HTTPMessage]:
        ids = [item.as_id() for item in self.items]
        return [FailedRequestItems(ids=ids, error=error_message)]


class ResponseList(UserList[ResponseMessage | FailedRequestMessage]):
    def __init__(self, collection: Sequence[ResponseMessage | FailedRequestMessage] | None = None) -> None:
        super().__init__(collection or [])

    def raise_for_status(self) -> None:
        """Raises an exception if any response in the list indicates a failure."""
        failed_responses = [resp for resp in self.data if isinstance(resp, FailedResponse)]
        failed_requests = [resp for resp in self.data if isinstance(resp, FailedRequestMessage)]
        if not failed_responses and not failed_requests:
            return
        error_messages = "; ".join(f"Status {err.status_code}: {err.error}" for err in failed_responses)
        if failed_requests:
            if error_messages:
                error_messages += "; "
            error_messages += "; ".join(f"Request error: {err.error}" for err in failed_requests)
        raise ToolkitAPIError(f"One or more requests failed: {error_messages}")

    def get_first_body(self) -> dict[str, JsonVal]:
        """Returns the body of the first successful response in the list.

        Raises:
            ValueError: If there are no successful responses with a body.

        Returns:
            dict[str, JsonVal]: The body of the first successful response.
        """
        for resp in self.data:
            if isinstance(resp, SuccessResponse) and resp.body is not None:
                return _json.loads(resp.body)
        raise ValueError("No successful responses with a body found.")


def _dump_body(body: dict[str, JsonVal]) -> str:
    try:
        return _json.dumps(body, allow_nan=False)
    except ValueError as e:
        # A lot of work to give a more human friendly error message when nans and infs are present:
        msg = "Out of range float values are not JSON compliant"
        if msg in str(e):  # exc. might e.g. contain an extra ": nan", depending on build (_json.make_encoder)
            raise ValueError(f"{msg}. Make sure your data does not contain NaN(s) or +/- Inf!").with_traceback(
                e.__traceback__
            ) from None
        raise
