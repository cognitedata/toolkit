from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, Identifier, RequestResource, ResponseResource

from .data_modeling._references import ContainerReference


class RecordIdentifier(Identifier):
    space: str
    external_id: str

    def __str__(self) -> str:
        return f"Record({self.space}, {self.external_id})"


class RecordSource(BaseModelObject):
    source: ContainerReference
    properties: dict[str, JsonValue]


class RecordRequest(RequestResource):
    """A record request resource for ingesting into a stream."""

    space: str
    external_id: str
    sources: list[RecordSource]

    def as_id(self) -> RecordIdentifier:
        return RecordIdentifier(space=self.space, external_id=self.external_id)


class RecordResponse(ResponseResource[RecordRequest]):
    """A record response from the API."""

    space: str
    external_id: str
    properties: dict[str, dict[str, JsonValue]] | None = None

    def as_request_resource(self) -> RecordRequest:
        raise NotImplementedError("Converting RecordResponse to RecordRequest is not yet supported.")
