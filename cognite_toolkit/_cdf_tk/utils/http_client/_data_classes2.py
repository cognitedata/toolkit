import gzip
import json
import sys
from abc import ABC, abstractmethod
from collections.abc import Hashable
from typing import Any, Generic, Literal, TypeVar

from cognite.client import global_config
from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator
from pydantic.alias_generators import to_camel

from cognite_toolkit._cdf_tk.utils.http_client._tracker import ItemsRequestTracker
from cognite_toolkit._cdf_tk.utils.useful_types import PrimitiveType

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class HTTPResult2(BaseModel): ...


class FailedRequestMessage2(HTTPResult2):
    error: str


class SuccessResponseMessage2(HTTPResult2):
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


class FailedResponseMessage2(HTTPResult2):
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

    @abstractmethod
    @property
    def content(self) -> str | bytes | None: ...


class RequestMessage2(BaseRequestMessage):
    data_content: bytes | None = None
    body_content: dict[str, JsonValue] | list[JsonValue] | None = None

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
            data = self.model_dump_json(include={"body_content"})
            if not global_config.disable_gzip:
                data = gzip.compress(data.encode("utf-8"))
        return data


class ItemsResultMessage2(BaseModel):
    ids: list[Hashable]


class ItemsFailedRequestMessage2(ItemsResultMessage2):
    error_message: str


class ItemsSuccessResponseMessage2(ItemsResultMessage2):
    status_code: int
    body: str
    content: bytes


class ItemsFailedResponseMessage2(ItemsResultMessage2):
    status_code: int
    error: ErrorDetails2
    body: str


class BaseModelObject(BaseModel):
    """Base class for all object. This includes resources and nested objects."""

    # We allow extra fields to support forward compatibility.
    model_config = ConfigDict(alias_generator=to_camel, extra="allow")

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the resource to a dictionary.

        This is the default serialization method for request resources.
        """
        return self.model_dump(mode="json", by_alias=camel_case)

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> "Self":
        """Load method to match CogniteResource signature."""
        return cls.model_validate(resource)


class RequestResource(BaseModelObject, ABC):
    @abstractmethod
    def as_id(self) -> Hashable: ...


T_RequestResource = TypeVar("T_RequestResource", bound=RequestResource)


def _set_default_tracker(data: dict[str, Any]) -> ItemsRequestTracker:
    if "tracker" not in data or data["tracker"] is None:
        return ItemsRequestTracker(data.get("max_failures_before_abort", 50))
    return data["tracker"]


class ItemsRequest2(BaseRequestMessage, Generic[T_RequestResource]):
    items: list[T_RequestResource]
    extra_body_fields: dict[str, JsonValue] | None = None
    max_failures_before_abort: int = 50
    tracker: ItemsRequestTracker = Field(init=False, default_factory=_set_default_tracker)

    @property
    def content(self) -> str:
        body: dict[str, Any] = {
            "items": [
                item.model_dump(exclude_unset=False, by_alias=True, exclude_none=False, mode="json")
                for item in self.items
            ]
        }
        if self.extra_body_fields:
            body.update(self.extra_body_fields)
        return json.dumps(body)

    def split(self, status_attempts: int) -> list["ItemsRequest2[T_RequestResource]"]:
        """Split the request into multiple requests with a single item each."""
        mid = len(self.items) // 2
        if mid == 0:
            return [self]
        self.tracker.register_failure()
        messages: list[ItemsRequest2[T_RequestResource]] = []
        for part in (self.items[:mid], self.items[mid:]):
            new_request = self.model_copy(update={"items": part, "status_attempt": status_attempts})
            new_request.tracker = self.tracker
            messages.append(new_request)
        return messages
