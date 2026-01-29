import gzip
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Literal

import httpx
from cognite.client import global_config
from pydantic import BaseModel, JsonValue, TypeAdapter, model_validator

from cognite_toolkit._cdf_tk.client.http_client._exception import ToolkitAPIError
from cognite_toolkit._cdf_tk.utils.useful_types import PrimitiveType

if TYPE_CHECKING:
    from cognite_toolkit._cdf_tk.client.http_client._item_classes import ItemsResultMessage


class HTTPResult(BaseModel):
    def get_success_or_raise(self) -> "SuccessResponse":
        """Raises an exception if any response in the list indicates a failure."""
        if isinstance(self, SuccessResponse):
            return self
        elif isinstance(self, FailedResponse):
            raise ToolkitAPIError(
                f"Request failed with status code {self.status_code}: {self.error.message}",
                missing=self.error.missing,  # type: ignore[arg-type]
                duplicated=self.error.duplicated,  # type: ignore[arg-type]
            )
        elif isinstance(self, FailedRequest):
            raise ToolkitAPIError(f"Request failed with error: {self.error}")
        else:
            raise ToolkitAPIError("Unknown HTTPResult2 type")

    def as_item_response(self, item_id: str) -> "ItemsResultMessage":
        # Avoid circular import
        from cognite_toolkit._cdf_tk.client.http_client._item_classes import (
            ItemsFailedRequest,
            ItemsFailedResponse,
            ItemsSuccessResponse,
        )

        if isinstance(self, SuccessResponse):
            return ItemsSuccessResponse(
                status_code=self.status_code, content=self.content, ids=[item_id], body=self.body
            )
        elif isinstance(self, FailedResponse):
            return ItemsFailedResponse(
                status_code=self.status_code,
                ids=[item_id],
                body=self.body,
                error=ErrorDetails(
                    code=self.error.code,
                    message=self.error.message,
                    missing=self.error.missing,
                    duplicated=self.error.duplicated,
                ),
            )
        elif isinstance(self, FailedRequest):
            return ItemsFailedRequest(ids=[item_id], error_message=self.error)
        else:
            raise ToolkitAPIError(f"Unknown {type(self).__name__} type")


class FailedRequest(HTTPResult):
    error: str


class SuccessResponse(HTTPResult):
    status_code: int
    body: str
    content: bytes

    @property
    def body_json(self) -> dict[str, Any]:
        """Parse the response body as JSON."""
        return TypeAdapter(dict[str, JsonValue]).validate_json(self.body)


class ErrorDetails(BaseModel):
    """This is the expected structure of error details in the CDF API"""

    code: int
    message: str
    missing: list[JsonValue] | None = None
    duplicated: list[JsonValue] | None = None
    is_auto_retryable: bool | None = None

    @classmethod
    def from_response(cls, response: httpx.Response) -> "ErrorDetails":
        """Populate the error details from a httpx response."""
        try:
            res = TypeAdapter(dict[Literal["error"], ErrorDetails]).validate_json(response.text)
        except ValueError:
            return cls(code=response.status_code, message=response.text)
        return res["error"]


class FailedResponse(HTTPResult):
    status_code: int
    body: str
    error: ErrorDetails


class BaseRequestMessage(BaseModel, ABC):
    endpoint_url: str
    method: Literal["GET", "POST", "PATCH", "DELETE", "PUT"]
    connect_attempt: int = 0
    read_attempt: int = 0
    status_attempt: int = 0
    api_version: str | None = None
    disable_gzip: bool = False
    content_length: int | None = None
    content_type: str = "application/json"
    accept: str = "application/json"

    parameters: dict[str, PrimitiveType] | None = None

    @property
    def total_attempts(self) -> int:
        return self.connect_attempt + self.read_attempt + self.status_attempt

    @property
    @abstractmethod
    def content(self) -> str | bytes | None: ...


class RequestMessage(BaseRequestMessage):
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
            if not global_config.disable_gzip and not self.disable_gzip:
                data = gzip.compress(data)
        elif self.body_content is not None:
            # We serialize using pydantic instead of json.dumps. This is because pydantic is faster
            # and handles more complex types such as datetime, float('nan'), etc.
            data = _BODY_SERIALIZER.dump_json(self.body_content)
            if not global_config.disable_gzip and not self.disable_gzip and isinstance(data, bytes):
                data = gzip.compress(data)
        return data


_BODY_SERIALIZER = TypeAdapter(dict[str, JsonValue])
