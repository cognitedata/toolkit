import gzip
import sys
from abc import ABC, abstractmethod
from collections import UserList
from collections.abc import Hashable, Sequence
from typing import Any, Literal

import httpx
from cognite.client import global_config
from pydantic import BaseModel, ConfigDict, Field, JsonValue, TypeAdapter, model_validator
from pydantic.alias_generators import to_camel

from cognite_toolkit._cdf_tk.utils.http_client._exception import ToolkitAPIError
from cognite_toolkit._cdf_tk.utils.http_client._tracker import ItemsRequestTracker
from cognite_toolkit._cdf_tk.utils.useful_types import PrimitiveType

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class HTTPResult2(BaseModel):
    def get_success_or_raise(self) -> "SuccessResponse2":
        """Raises an exception if any response in the list indicates a failure."""
        if isinstance(self, SuccessResponse2):
            return self
        elif isinstance(self, FailedResponse2):
            raise ToolkitAPIError(
                f"Request failed with status code {self.status_code}: {self.error.code} - {self.error.message}"
            )
        elif isinstance(self, FailedRequest2):
            raise ToolkitAPIError(f"Request failed with error: {self.error}")
        else:
            raise ToolkitAPIError("Unknown HTTPResult2 type")


class FailedRequest2(HTTPResult2):
    error: str


class SuccessResponse2(HTTPResult2):
    status_code: int
    body: str
    content: bytes

    @property
    def body_json(self) -> dict[str, Any]:
        """Parse the response body as JSON."""
        return TypeAdapter(dict[str, JsonValue]).validate_json(self.body)


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
            data = _BODY_SERIALIZER.dump_json(self.body_content)
            if not global_config.disable_gzip and isinstance(data, bytes):
                data = gzip.compress(data)
        return data


_BODY_SERIALIZER = TypeAdapter(dict[str, JsonValue])


class ItemsResultMessage2(BaseModel):
    ids: list[Hashable]


class ItemsFailedRequest2(ItemsResultMessage2):
    error_message: str


class ItemsSuccessResponse2(ItemsResultMessage2):
    status_code: int
    body: str
    content: bytes


class ItemsFailedResponse2(ItemsResultMessage2):
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
        return self.model_dump(mode="json", by_alias=camel_case, exclude_unset=True)

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> "Self":
        """Load method to match CogniteResource signature."""
        return cls.model_validate(resource)


class RequestResource(BaseModelObject, ABC):
    @abstractmethod
    def as_id(self) -> Hashable: ...


def _set_default_tracker(data: dict[str, Any]) -> ItemsRequestTracker:
    if "tracker" not in data or data["tracker"] is None:
        return ItemsRequestTracker(data.get("max_failures_before_abort", 50))
    return data["tracker"]


class ItemsRequest2(BaseRequestMessage):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    items: Sequence[RequestResource]
    extra_body_fields: dict[str, JsonValue] | None = None
    max_failures_before_abort: int = 50
    tracker: ItemsRequestTracker = Field(init=False, default_factory=_set_default_tracker, exclude=True)

    @property
    def content(self) -> str | bytes | None:
        body: dict[str, JsonValue] = {"items": [item.dump() for item in self.items]}
        if self.extra_body_fields:
            body.update(self.extra_body_fields)
        res = _BODY_SERIALIZER.dump_json(body)
        if not global_config.disable_gzip and isinstance(res, bytes):
            return gzip.compress(res)
        return res

    def split(self, status_attempts: int) -> list["ItemsRequest2"]:
        """Split the request into multiple requests with a single item each."""
        mid = len(self.items) // 2
        if mid == 0:
            return [self]
        self.tracker.register_failure()
        messages: list[ItemsRequest2] = []
        for part in (self.items[:mid], self.items[mid:]):
            new_request = self.model_copy(update={"items": part, "status_attempt": status_attempts})
            new_request.tracker = self.tracker
            messages.append(new_request)
        return messages


class ItemResponse(BaseModel):
    items: list[dict[str, JsonValue]]


class ItemsResultList(UserList[ItemsResultMessage2]):
    def __init__(self, collection: Sequence[ItemsResultMessage2] | None = None) -> None:
        super().__init__(collection or [])

    def raise_for_status(self) -> None:
        """Raises an exception if any response in the list indicates a failure."""
        failed_responses = [resp for resp in self.data if isinstance(resp, ItemsFailedResponse2)]
        failed_requests = [resp for resp in self.data if isinstance(resp, ItemsFailedRequest2)]
        if not failed_responses and not failed_requests:
            return
        error_messages = "; ".join(f"Status {err.status_code}: {err.error.message}" for err in failed_responses)
        if failed_requests:
            if error_messages:
                error_messages += "; "
            error_messages += "; ".join(f"Request error: {err.error_message}" for err in failed_requests)
        raise ToolkitAPIError(f"One or more requests failed: {error_messages}")

    @property
    def has_failed(self) -> bool:
        """Indicates whether any response in the list indicates a failure.

        Returns:
            bool: True if there are any failed responses or requests, False otherwise.
        """
        for resp in self.data:
            if isinstance(resp, ItemsFailedResponse2 | ItemsFailedRequest2):
                return True
        return False

    def get_items(self) -> list[dict[str, JsonValue]]:
        """Get the items from all successful responses."""
        items: list[dict[str, JsonValue]] = []
        for resp in self.data:
            if isinstance(resp, ItemsSuccessResponse2):
                body_json = ItemResponse.model_validate_json(resp.body)
                items.extend(body_json.items)
        return items
