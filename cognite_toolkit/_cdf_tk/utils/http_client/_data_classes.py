from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Literal, TypeAlias

from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

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


@dataclass
class BodyRequestMessage(RequestMessage, ABC):
    """Base class for HTTP request messages with a body"""

    @abstractmethod
    def body(self) -> dict[str, JsonVal]:
        raise NotImplementedError()


@dataclass
class ItemsRequestMessage(BodyRequestMessage):
    items: list[dict[str, JsonVal]] = field(default_factory=list)
    extra_body_fields: dict[str, JsonVal] = field(default_factory=dict)

    def body(self) -> dict[str, JsonVal]:
        if self.extra_body_fields:
            return {"items": self.items, **self.extra_body_fields}
        return {"items": self.items}


@dataclass
class ResponseMessage(HTTPMessage):
    status_code: StatusCode


@dataclass
class FailedResponseMessage(ResponseMessage):
    error: str


@dataclass
class ItemsResponseMessage(ResponseMessage):
    items: list[dict[str, JsonVal]] = field(default_factory=list)
    extra_response_fields: dict[str, JsonVal] = field(default_factory=dict)
