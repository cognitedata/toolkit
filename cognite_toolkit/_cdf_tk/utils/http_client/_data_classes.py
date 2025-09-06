import sys
from abc import ABC, abstractmethod
from collections.abc import Callable, Hashable
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
    def create_success(self, response: requests.Response) -> list[HTTPMessage]:
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
    def create_success(cls, response: requests.Response) -> list[HTTPMessage]:
        return [ResponseMessage(status_code=response.status_code)]


@dataclass
class ItemsRequestMessage(Generic[T_ID], BodyRequestMessage):
    items: list[JsonVal] = field(default_factory=list)
    extra_body_fields: dict[str, JsonVal] = field(default_factory=dict)
    as_id: Callable[[dict[str, JsonVal]], T_ID] | None = None

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

    def create_success(self, response: requests.Response) -> list[HTTPMessage]:
        raise NotImplementedError()


@dataclass
class ResponseMessage(HTTPMessage):
    status_code: StatusCode


@dataclass
class SimpleResponseMessage(ResponseMessage):
    body: dict[str, JsonVal] | None = None


@dataclass
class FailedResponseMessage(ResponseMessage):
    error: str


@dataclass
class ItemResponseMessage(Generic[T_ID], ResponseMessage):
    id: T_ID


@dataclass
class SuccessItemMessage(ItemResponseMessage):
    item: dict[str, JsonVal] | None = None


@dataclass
class FailedItemMessage(ItemResponseMessage):
    error: str


@dataclass
class MissingItemResponseMessage(ItemResponseMessage): ...


@dataclass
class ItemsResponseMessage(ResponseMessage):
    items: list[SuccessItemMessage] = field(default_factory=list)
    extra_response_fields: dict[str, JsonVal] = field(default_factory=dict)
