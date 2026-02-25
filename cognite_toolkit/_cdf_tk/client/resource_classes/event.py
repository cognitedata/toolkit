import builtins
from typing import ClassVar

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
    UpdatableRequestResource,
)
from cognite_toolkit._cdf_tk.client._types import Metadata

from .identifiers import ExternalId, InternalOrExternalId


class Event(BaseModelObject):
    external_id: str | None = None
    data_set_id: int | None = None
    start_time: int | None = None
    end_time: int | None = None
    type: str | None = None
    subtype: str | None = None
    description: str | None = None
    metadata: Metadata | None = None
    asset_ids: list[int] | None = None
    source: str | None = None

    def as_id(self) -> InternalOrExternalId:
        if self.external_id is None:
            raise ValueError("Cannot convert EventRequest to ExternalId when external_id is None")
        return ExternalId(external_id=self.external_id)


class EventRequest(Event, UpdatableRequestResource):
    container_fields: ClassVar[frozenset[str]] = frozenset({"metadata", "asset_ids"})


class EventResponse(Event, ResponseResource[EventRequest]):
    id: int
    created_time: int
    last_updated_time: int

    @classmethod
    def request_cls(cls) -> builtins.type[EventRequest]:
        return EventRequest
