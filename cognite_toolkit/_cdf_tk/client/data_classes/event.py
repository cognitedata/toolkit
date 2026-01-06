from cognite_toolkit._cdf_tk.client.data_classes.base import RequestResource, ResponseResource

from .identifiers import ExternalId, InternalOrExternalId


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

    def as_id(self) -> InternalOrExternalId:
        return ExternalId(external_id=self.external_id or "<missing>")


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
