import sys
from abc import ABC, abstractmethod
from collections.abc import Callable, Hashable, Sequence
from dataclasses import dataclass, field
from typing import Generic, Literal, TypeAlias, TypeVar

import requests

from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

if sys.version_info >= (3, 11):
    pass
else:
    pass

T_ID = TypeVar("T_ID", bound=Hashable)
StatusCode: TypeAlias = int


@dataclass
class HTTPMessage(ABC):
    """Base class for HTTP messages (requests and responses)"""

    ...


@dataclass
class FailedRequest(HTTPMessage):
    error: str


@dataclass
class ResponseMessage(HTTPMessage):
    status_code: StatusCode


@dataclass
class SuccessResponseMessage(ResponseMessage):
    body: dict[str, JsonVal] | None = None


@dataclass
class FailedResponseMessage(ResponseMessage):
    error: str


@dataclass
class ItemResponseMessage(Generic[T_ID], ResponseMessage):
    id: T_ID

    @classmethod
    def create_from_response(
        cls,
        response: requests.Response,
        response_body: dict[str, JsonVal] | None = None,
        error_message: str | None = None,
    ) -> "Sequence[ItemResponseMessage[T_ID]]":
        raise NotImplementedError()


@dataclass
class SuccessItem(ItemResponseMessage):
    item: JsonVal | None = None


@dataclass
class FailedItem(ItemResponseMessage):
    error: str


@dataclass
class MissingItem(ItemResponseMessage): ...


@dataclass
class UnexpectedItem(ItemResponseMessage):
    item: JsonVal | None = None


@dataclass
class UnknownItem(ItemResponseMessage):
    as_id_error: str
    message: Literal["request", "response"]
    item: JsonVal | None = None


@dataclass
class FailedRequestItem(ItemResponseMessage):
    error: str


@dataclass
class RequestMessage(HTTPMessage):
    """Base class for HTTP request messages"""

    endpoint_url: str
    method: Literal["GET", "POST", "PATCH", "DELETE"]
    connect_attempt: int = 0
    read_attempt: int = 0
    status_attempt: int = 0

    @property
    def total_attempts(self) -> int:
        return self.connect_attempt + self.read_attempt + self.status_attempt

    @abstractmethod
    def create_responses(
        self,
        response: requests.Response,
        response_body: dict[str, JsonVal] | None = None,
        error_message: str | None = None,
    ) -> Sequence[HTTPMessage]:
        raise NotImplementedError()

    @abstractmethod
    def create_failed(self, error_message: str) -> Sequence[HTTPMessage]:
        raise NotImplementedError()


@dataclass
class BodyRequestMessage(RequestMessage, ABC):
    """Base class for HTTP request messages with a body"""

    @abstractmethod
    def body(self) -> dict[str, JsonVal]:
        raise NotImplementedError()


@dataclass
class SimpleBodyRequestMessage(BodyRequestMessage):
    body_content: dict[str, JsonVal] = field(default_factory=dict)

    def body(self) -> dict[str, JsonVal]:
        return self.body_content

    @classmethod
    def create_responses(
        cls,
        response: requests.Response,
        response_body: dict[str, JsonVal] | None = None,
        error_message: str | None = None,
    ) -> Sequence[ResponseMessage]:
        if 200 <= response.status_code < 300:
            return [SuccessResponseMessage(status_code=response.status_code, body=response_body)]
        if error_message is None:
            error_message = f"Request failed with status code {response.status_code}"
        return [FailedResponseMessage(status_code=response.status_code, error=error_message)]

    def create_failed(self, error_message: str) -> Sequence[HTTPMessage]:
        return [FailedRequest(error=error_message)]


@dataclass
class ItemsRequestMessage(Generic[T_ID], BodyRequestMessage):
    items: list[JsonVal] = field(default_factory=list)
    extra_body_fields: dict[str, JsonVal] = field(default_factory=dict)
    as_id: Callable[[JsonVal], T_ID] | None = None

    def body(self) -> dict[str, JsonVal]:
        if self.extra_body_fields:
            return {"items": self.items, **self.extra_body_fields}
        return {"items": self.items}

    def split(self, status_attempts: int) -> list[HTTPMessage]:
        mid = len(self.items) // 2
        if mid == 0:
            return [self]
        first_half = ItemsRequestMessage[T_ID](
            endpoint_url=self.endpoint_url,
            method=self.method,
            items=self.items[:mid],
            extra_body_fields=self.extra_body_fields,
            as_id=self.as_id,
            connect_attempt=self.connect_attempt,
            read_attempt=self.read_attempt,
            status_attempt=status_attempts,
        )
        second_half = ItemsRequestMessage[T_ID](
            endpoint_url=self.endpoint_url,
            method=self.method,
            items=self.items[mid:],
            extra_body_fields=self.extra_body_fields,
            as_id=self.as_id,
            connect_attempt=self.connect_attempt,
            read_attempt=self.read_attempt,
            status_attempt=status_attempts,
        )
        return [first_half, second_half]

    def create_responses(
        self,
        response: requests.Response,
        response_body: dict[str, JsonVal] | None = None,
        error_message: str | None = None,
    ) -> Sequence[HTTPMessage]:
        if self.as_id is None:
            return SimpleBodyRequestMessage.create_responses(response, response_body, error_message)
        responses: list[ItemResponseMessage] = []
        items_by_id: dict[T_ID, JsonVal] = {}
        for item in self.items:
            try:
                item_id = self.as_id(item)
            except Exception as e:
                responses.append(
                    UnknownItem(
                        status_code=response.status_code,
                        id=None,
                        item=item,
                        message="request",
                        as_id_error=f"Error extracting ID: {e!s}",
                    )
                )
                continue
            if item_id in items_by_id:
                responses.append(
                    FailedItem(
                        status_code=response.status_code,
                        id=item_id,
                        error="Duplicate ID in request items",
                    )
                )
            else:
                items_by_id[item_id] = item

        if response_body is None or not isinstance(response_body["items"], list):
            responses.extend(ItemResponseMessage.create_from_response(response, response_body, error_message))
            return responses
        for response_item in response_body["items"]:
            if not isinstance(response_item, dict):
                responses.append(
                    UnknownItem(
                        status_code=response.status_code,
                        id=None,
                        item=response_item,
                        message="response",
                        as_id_error=f"Response item is not a dictionary, got {type(response_item).__name__}",
                    )
                )
                continue
            try:
                item_id = self.as_id(response_item)
            except Exception as e:
                responses.append(
                    UnknownItem(
                        status_code=response.status_code,
                        id=None,
                        item=response_item,
                        message="response",
                        as_id_error=f"Error extracting ID: {e!s}",
                    )
                )
                continue
            request_item = items_by_id.pop(item_id, None)
            if request_item is None:
                responses.append(
                    UnexpectedItem(
                        status_code=response.status_code,
                        id=item_id,
                        item=response_item,
                    )
                )
            if 200 <= response.status_code < 300:
                responses.append(
                    SuccessItem(
                        status_code=response.status_code,
                        id=item_id,
                        item=response_item,
                    )
                )
            else:
                error = error_message or f"Request failed with status code {response.status_code}"
                responses.append(
                    FailedItem(
                        status_code=response.status_code,
                        id=item_id,
                        error=error,
                    )
                )

        for item_id, request_item in items_by_id.items():
            responses.append(
                MissingItem(
                    status_code=response.status_code,
                    id=item_id,
                )
            )

        return responses

    def create_failed(self, error_message: str) -> Sequence[HTTPMessage]:
        if self.as_id is None:
            return [FailedRequest(error=error_message)]
        results: list[HTTPMessage] = []
        for req_item in self.items:
            try:
                item_id = self.as_id(req_item)
            except Exception as e:
                results.append(
                    UnknownItem(id=None, status_code=-1, as_id_error=str(e), message="request", item=req_item)
                )
            else:
                results.append(FailedRequestItem(id=item_id, status_code=-1, error=error_message))
        return results
