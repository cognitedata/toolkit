"""Data classes for handling item-based requests and responses in the Cognite Toolkit HTTP client."""

import gzip
from collections import UserList
from collections.abc import Sequence
from typing import Any

from cognite.client import global_config
from pydantic import BaseModel, ConfigDict, Field, JsonValue

from cognite_toolkit._cdf_tk.client._resource_base import RequestItem
from cognite_toolkit._cdf_tk.client.http_client._data_classes import (
    _BODY_SERIALIZER,
    BaseRequestMessage,
    ErrorDetails,
)
from cognite_toolkit._cdf_tk.client.http_client._exception import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.http_client._tracker import ItemsRequestTracker


class ItemsResultMessage(BaseModel):
    ids: list[str]


class ItemsFailedRequest(ItemsResultMessage):
    error_message: str


class ItemsSuccessResponse(ItemsResultMessage):
    status_code: int
    body: str
    content: bytes


class ItemsFailedResponse(ItemsResultMessage):
    status_code: int
    error: ErrorDetails
    body: str


def _set_default_tracker(data: dict[str, Any]) -> ItemsRequestTracker:
    if "tracker" not in data or data["tracker"] is None:
        return ItemsRequestTracker(data.get("max_failures_before_abort", 50))
    return data["tracker"]


class ItemsRequest(BaseRequestMessage):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    items: Sequence[RequestItem]
    extra_body_fields: dict[str, JsonValue] | None = None
    max_failures_before_abort: int = 50
    tracker: ItemsRequestTracker = Field(init=False, default_factory=_set_default_tracker, exclude=True)

    @property
    def content(self) -> str | bytes | None:
        body: dict[str, JsonValue] = {"items": [item.dump(camel_case=True) for item in self.items]}
        if self.extra_body_fields:
            body.update(self.extra_body_fields)
        res = _BODY_SERIALIZER.dump_json(body)
        if not global_config.disable_gzip and not self.disable_gzip and isinstance(res, bytes):
            return gzip.compress(res)
        return res

    def split(self, status_attempts: int) -> list["ItemsRequest"]:
        """Split the request into multiple requests with a single item each."""
        mid = len(self.items) // 2
        if mid == 0:
            return [self]
        self.tracker.register_failure()
        messages: list[ItemsRequest] = []
        for part in (self.items[:mid], self.items[mid:]):
            new_request = self.model_copy(update={"items": part, "status_attempt": status_attempts})
            new_request.tracker = self.tracker
            messages.append(new_request)
        return messages


class ItemResponse(BaseModel):
    items: list[dict[str, JsonValue]]


class ItemsResultList(UserList[ItemsResultMessage]):
    def __init__(self, collection: Sequence[ItemsResultMessage] | None = None) -> None:
        super().__init__(collection or [])

    def raise_for_status(self) -> None:
        """Raises an exception if any response in the list indicates a failure."""
        failed_responses = [resp for resp in self.data if isinstance(resp, ItemsFailedResponse)]
        failed_requests = [resp for resp in self.data if isinstance(resp, ItemsFailedRequest)]
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
            if isinstance(resp, ItemsFailedResponse | ItemsFailedRequest):
                return True
        return False

    def get_items(self) -> list[dict[str, JsonValue]]:
        """Get the items from all successful responses."""
        items: list[dict[str, JsonValue]] = []
        for resp in self.data:
            if isinstance(resp, ItemsSuccessResponse):
                body_json = ItemResponse.model_validate_json(resp.body)
                items.extend(body_json.items)
        return items
