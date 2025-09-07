from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Literal, TypeAlias

import requests

from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

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
class SuccessResponse(ResponseMessage):
    body: dict[str, JsonVal] | None = None


@dataclass
class FailedResponse(ResponseMessage):
    error: str


@dataclass
class SimpleRequest(RequestMessage):
    """Base class for HTTP request messages without a body"""

    @classmethod
    def create_responses(
        cls,
        response: requests.Response,
        response_body: dict[str, JsonVal] | None = None,
        error_message: str | None = None,
    ) -> Sequence[ResponseMessage]:
        if 200 <= response.status_code < 300 and error_message is None:
            return [SuccessResponse(status_code=response.status_code, body=response_body)]
        if error_message is None:
            error_message = f"Request failed with status code {response.status_code}"
        return [FailedResponse(status_code=response.status_code, error=error_message)]

    def create_failed(self, error_message: str) -> Sequence[HTTPMessage]:
        return [FailedRequest(error=error_message)]


@dataclass
class BodyRequestMessage(RequestMessage, ABC):
    """Base class for HTTP request messages with a body"""

    @abstractmethod
    def body(self) -> dict[str, JsonVal]:
        raise NotImplementedError()


@dataclass
class ParamRequest(SimpleRequest):
    """Base class for HTTP request messages with query parameters"""

    parameters: dict[str, str] | None = None


@dataclass
class SimpleBodyRequest(SimpleRequest, BodyRequestMessage):
    body_content: dict[str, JsonVal] = field(default_factory=dict)

    def body(self) -> dict[str, JsonVal]:
        return self.body_content
