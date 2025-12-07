import gzip
import sys
from abc import ABC, abstractmethod
from typing import Any, Literal

import httpx
from cognite.client import global_config
from pydantic import BaseModel, JsonValue, TypeAdapter, model_validator

from cognite_toolkit._cdf_tk.utils.useful_types import PrimitiveType

if sys.version_info >= (3, 11):
    pass
else:
    pass


class HTTPResult2(BaseModel): ...


class FailedRequest2(HTTPResult2):
    error: str


class SuccessResponse2(HTTPResult2):
    status_code: int
    body: str
    content: bytes


class ErrorDetails2(BaseModel):
    """This is the expected structure of error details in the CDF API"""

    code: int
    message: str
    missing: list[JsonValue] | None = None
    duplicated: list[JsonValue] | None = None
    is_auto_retryable: bool | None = None

    @classmethod
    def from_response(cls, response: httpx.Response) -> "ErrorDetails2":
        """Populate the error details from a httpx response."""
        try:
            res = TypeAdapter(dict[Literal["error"], ErrorDetails2]).validate_json(response.text)
        except ValueError:
            return cls(code=response.status_code, message=response.text)
        return res["error"]


class FailedResponse2(HTTPResult2):
    status_code: int
    body: str
    error: ErrorDetails2


class BaseRequestMessage(BaseModel, ABC):
    endpoint_url: str
    method: Literal["GET", "POST", "PATCH", "DELETE", "PUT"]
    connect_attempt: int = 0
    read_attempt: int = 0
    status_attempt: int = 0
    api_version: str | None = None
    content_type: str = "application/json"
    accept: str = "application/json"

    parameters: dict[str, PrimitiveType] | None = None

    @property
    def total_attempts(self) -> int:
        return self.connect_attempt + self.read_attempt + self.status_attempt

    @property
    @abstractmethod
    def content(self) -> str | bytes | None: ...


class RequestMessage2(BaseRequestMessage):
    data_content: bytes | None = None
    body_content: dict[str, JsonValue] | None = None

    @model_validator(mode="before")
    def check_data_or_body(cls, values: dict[str, Any]) -> dict[str, Any]:
        if values.get("data_content") is not None and values.get("body_content") is not None:
            raise ValueError("Only one of data_content or body_content can be set.")
        return values

    @property
    def content(self) -> str | bytes | None:
        data: str | bytes | None = None
        if self.data_content is not None:
            data = self.data_content
            if not global_config.disable_gzip:
                data = gzip.compress(data)
        elif self.body_content is not None:
            # We serialize using pydantic instead of json.dumps. This is because pydantic is faster
            # and handles more complex types such as datetime, float('nan'), etc.
            data = self.model_dump_json(include={"body_content"}).removesuffix("}").removeprefix('{"body_content":')
            if not global_config.disable_gzip:
                data = gzip.compress(data.encode("utf-8"))
        return data
