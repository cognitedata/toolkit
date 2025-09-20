from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Generic, Literal, TypeAlias

import httpx

from cognite_toolkit._cdf_tk.utils.http_client._tracker import ItemsRequestTracker
from cognite_toolkit._cdf_tk.utils.useful_types import T_ID, JsonVal

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
    def create_responses(
        self,
        response: httpx.Response,
        response_body: dict[str, JsonVal] | None = None,
        error_message: str | None = None,
    ) -> Sequence[HTTPMessage]:
        raise NotImplementedError()

    @abstractmethod
    def create_failed_request(self, error_message: str) -> Sequence[HTTPMessage]:
        raise NotImplementedError()


@dataclass
class SuccessResponse(ResponseMessage):
    body: dict[str, JsonVal] | None = None


@dataclass
class FailedResponse(ResponseMessage):
    error: str
    body: dict[str, JsonVal] | None = None


@dataclass
class SimpleRequest(RequestMessage):
    """Base class for requests with a simple success/fail response structure"""

    @classmethod
    def create_responses(
        cls,
        response: httpx.Response,
        response_body: dict[str, JsonVal] | None = None,
        error_message: str | None = None,
    ) -> Sequence[ResponseMessage]:
        if 200 <= response.status_code < 300 and error_message is None:
            return [SuccessResponse(status_code=response.status_code, body=response_body)]
        if error_message is None:
            error_message = f"Request failed with status code {response.status_code}"
        return [FailedResponse(status_code=response.status_code, error=error_message, body=response_body)]

    @classmethod
    def create_failed_request(cls, error_message: str) -> Sequence[HTTPMessage]:
        return [FailedRequestMessage(error=error_message)]


@dataclass
class BodyRequest(RequestMessage, ABC):
    """Base class for HTTP request messages with a body"""

    @abstractmethod
    def body(self) -> dict[str, JsonVal]:
        raise NotImplementedError()


@dataclass
class ParamRequest(SimpleRequest):
    """Base class for HTTP request messages with query parameters"""

    parameters: dict[str, str] | None = None


@dataclass
class SimpleBodyRequest(SimpleRequest, BodyRequest):
    body_content: dict[str, JsonVal] = field(default_factory=dict)

    def body(self) -> dict[str, JsonVal]:
        return self.body_content


@dataclass
class ItemMessage(ABC):
    """Base class for message related to a specific item"""

    ...


@dataclass
class ItemIDMessage(Generic[T_ID], ItemMessage, ABC):
    """Base class for message related to a specific item identified by an ID"""

    id: T_ID


@dataclass
class ItemResponse(ItemIDMessage, ResponseMessage, ABC): ...


@dataclass
class SuccessItem(ItemResponse):
    item: JsonVal | None = None


@dataclass
class FailedItem(ItemResponse):
    error: str


@dataclass
class MissingItem(ItemResponse): ...


@dataclass
class UnexpectedItem(ItemResponse):
    item: JsonVal | None = None


@dataclass
class FailedRequestItem(ItemIDMessage, FailedRequestMessage): ...


@dataclass
class UnknownRequestItem(ItemMessage, FailedRequestMessage):
    item: JsonVal | None = None


@dataclass
class UnknownResponseItem(ItemMessage, ResponseMessage):
    error: str
    item: JsonVal | None = None


@dataclass
class ItemsRequest(Generic[T_ID], BodyRequest):
    """Requests message for endpoints that accept multiple items in a single request.

    This class provides functionality to split large requests into smaller ones, handle responses for each item,
    and manage errors effectively.

    Attributes:
        items (list[JsonVal]): The list of items to be sent in the request body.
        extra_body_fields (dict[str, JsonVal]): Additional fields to include in the request body
        as_id (Callable[[JsonVal], T_ID] | None): A function to extract the ID from each item. If None, IDs are not used.
        max_failures_before_abort (int): The maximum number of failed split requests before aborting further splits.

    """

    items: list[JsonVal] = field(default_factory=list)
    extra_body_fields: dict[str, JsonVal] = field(default_factory=dict)
    as_id: Callable[[JsonVal], T_ID] | None = None
    max_failures_before_abort: int = 50
    tracker: ItemsRequestTracker | None = field(default=None, init=False)

    def dump(self) -> dict[str, JsonVal]:
        """Dumps the message to a JSON serializable dictionary.

        This override removes the 'as_id' attribute as it is not serializable.

        Returns:
            dict[str, JsonVal]: The message as a dictionary.
        """
        output = super().dump()
        if self.as_id is not None:
            # We cannot serialize functions
            del output["as_id"]
        if self.tracker is not None:
            # We cannot serialize the tracker
            del output["tracker"]
        return output

    def body(self) -> dict[str, JsonVal]:
        if self.extra_body_fields:
            return {"items": self.items, **self.extra_body_fields}
        return {"items": self.items}

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
            as_id=self.as_id,
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
            as_id=self.as_id,
            connect_attempt=self.connect_attempt,
            read_attempt=self.read_attempt,
            status_attempt=status_attempts,
        )
        second_half.tracker = tracker
        return [first_half, second_half]

    def create_responses(
        self,
        response: httpx.Response,
        response_body: dict[str, JsonVal] | None = None,
        error_message: str | None = None,
    ) -> Sequence[HTTPMessage]:
        """Creates response messages based on the HTTP response and the original request.

        Args:
            response: The HTTP response received from the server.
            response_body: The parsed JSON body of the response, if available.
            error_message: An optional error message to use if the response indicates a failure.

        Returns:
            A sequence of HTTPMessage instances representing the outcome for each item in the request.
        """
        if self.as_id is None:
            return SimpleBodyRequest.create_responses(response, response_body, error_message)
        request_items_by_id, errors = self._create_items_by_id()
        responses: list[HTTPMessage] = list(errors)
        error_message = error_message or "Unknown error"

        if not self._is_items_response(response_body):
            return self._handle_non_items_response(responses, response, error_message, request_items_by_id)

        # Process items from response
        if response_body is not None:
            self._process_response_items(responses, response, response_body, error_message, request_items_by_id)

        # Handle missing items
        self._handle_missing_items(responses, response, request_items_by_id)

        return responses

    @staticmethod
    def _handle_non_items_response(
        responses: list[HTTPMessage],
        response: httpx.Response,
        error_message: str,
        request_items_by_id: dict[T_ID, JsonVal],
    ) -> list[HTTPMessage]:
        """Handles responses that do not contain an 'items' field in the body."""
        if 200 <= response.status_code < 300:
            responses.extend(
                SuccessItem(status_code=response.status_code, id=id_) for id_ in request_items_by_id.keys()
            )
        else:
            responses.extend(
                FailedItem(status_code=response.status_code, error=error_message, id=id_)
                for id_ in request_items_by_id.keys()
            )
        return responses

    def _process_response_items(
        self,
        responses: list[HTTPMessage],
        response: httpx.Response,
        response_body: dict[str, JsonVal],
        error_message: str,
        request_items_by_id: dict[T_ID, JsonVal],
    ) -> None:
        """Processes each item in the response body and categorizes them based on their status."""
        for response_item in response_body["items"]:  # type: ignore[union-attr]
            try:
                item_id = self.as_id(response_item)  # type: ignore[misc]
            except Exception as e:
                responses.append(
                    UnknownResponseItem(
                        status_code=response.status_code, item=response_item, error=f"Error extracting ID: {e!s}"
                    )
                )
                continue
            request_item = request_items_by_id.pop(item_id, None)
            if request_item is None:
                responses.append(UnexpectedItem(status_code=response.status_code, id=item_id, item=response_item))
            elif 200 <= response.status_code < 300:
                responses.append(SuccessItem(status_code=response.status_code, id=item_id, item=response_item))
            else:
                responses.append(FailedItem(status_code=response.status_code, id=item_id, error=error_message))

    @staticmethod
    def _handle_missing_items(
        responses: list[HTTPMessage], response: httpx.Response, request_items_by_id: dict[T_ID, JsonVal]
    ) -> None:
        """Handles items that were in the request but not present in the response."""
        for item_id in request_items_by_id.keys():
            responses.append(MissingItem(status_code=response.status_code, id=item_id))

    def create_failed_request(self, error_message: str) -> Sequence[HTTPMessage]:
        if self.as_id is None:
            return SimpleBodyRequest.create_failed_request(error_message)
        items_by_id, errors = self._create_items_by_id()
        results: list[HTTPMessage] = []
        results.extend(errors)
        results.extend(FailedRequestItem(id=item_id, error=error_message) for item_id in items_by_id.keys())
        return results

    def _create_items_by_id(self) -> tuple[dict[T_ID, JsonVal], list[FailedRequestItem | UnknownRequestItem]]:
        if self.as_id is None:
            raise ValueError("as_id function must be provided to create items by ID")
        items_by_id: dict[T_ID, JsonVal] = {}
        errors: list[FailedRequestItem | UnknownRequestItem] = []
        for item in self.items:
            try:
                item_id = self.as_id(item)
            except Exception as e:
                errors.append(UnknownRequestItem(error=f"Error extracting ID: {e!s}", item=item))
                continue
            if item_id in items_by_id:
                errors.append(FailedRequestItem(id=item_id, error=f"Duplicate item ID: {item_id!r}"))
            else:
                items_by_id[item_id] = item
        return items_by_id, errors

    @staticmethod
    def _is_items_response(body: dict[str, JsonVal] | None = None) -> bool:
        if body is None:
            return False
        if "items" not in body:
            return False
        if not isinstance(body["items"], list):
            return False
        return True
