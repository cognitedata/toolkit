from collections.abc import Hashable

from cognite_toolkit._cdf_tk.client.data_classes.base import ResponseResource
from cognite_toolkit._cdf_tk.client.http_client import RequestResource


class EventRequest(RequestResource):
    external_id: str | None = None
    data_set_id: int | None = None
    start_time: int | None = None
    end_time: int | None = None
    type: str | None = None
    subtype: str | None = None
    description: str | None = None
    metadata: dict[str, str] | None = None
    asset_ids: list[int] | None = None
    source: str | None = None

    def as_id(self) -> Hashable:
        return self.external_id


class EventResponse(ResponseResource[EventRequest]):
    external_id: str | None = None
    data_set_id: int | None = None
    start_time: int | None = None
    end_time: int | None = None
    type: str | None = None
    subtype: str | None = None
    description: str | None = None
    metadata: dict[str, str] | None = None
    asset_ids: list[int] | None = None
    source: str | None = None
    id: int
    created_time: int
    last_updated_time: int

    def as_request_resource(self) -> EventRequest:
        return EventRequest.model_validate(self.dump())
