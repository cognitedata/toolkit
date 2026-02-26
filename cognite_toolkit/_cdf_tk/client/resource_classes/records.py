from typing import Any

from pydantic import JsonValue, field_serializer, field_validator

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, Identifier, RequestResource, ResponseResource
from cognite_toolkit._cdf_tk.client.identifiers import ContainerReference


class RecordIdentifier(Identifier):
    space: str
    external_id: str

    def __str__(self) -> str:
        return f"Record({self.space}, {self.external_id})"


class RecordSource(BaseModelObject):
    source: ContainerReference
    properties: dict[str, JsonValue]

    @field_serializer("source", mode="plain")
    def serialize_source(self, value: ContainerReference) -> Any:
        return {**value.dump(), "type": value.type}


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
    properties: dict[ContainerReference, dict[str, JsonValue]] | None = None

    @field_validator("properties", mode="before")
    def parse_reference(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        parsed: dict[ContainerReference, dict[str, JsonValue]] = {}
        for space, inner_dict in value.items():
            if isinstance(space, ContainerReference):
                parsed[space] = inner_dict
                continue
            if not isinstance(inner_dict, dict) or not isinstance(space, str):
                raise ValueError(
                    f"Invalid properties format expected dict[str, dict[...]], "
                    f"got: dict[{type(space).__name__}, {type(inner_dict).__name__}]"
                )
            for container_external_id, props in inner_dict.items():
                source_ref = ContainerReference(space=space, external_id=container_external_id)  # pyright: ignore[reportCallIssue]
                parsed[source_ref] = props
        return parsed

    @field_serializer("properties", mode="plain")
    def serialize_properties(
        self, value: dict[ContainerReference, dict[str, JsonValue]] | None
    ) -> dict[str, dict[str, dict[str, JsonValue]]] | None:
        if value is None:
            return None
        serialized: dict[str, dict[str, dict[str, JsonValue]]] = {}
        for source_ref, props in value.items():
            space = source_ref.space
            if space not in serialized:
                serialized[space] = {}
            serialized[space][source_ref.external_id] = props
        return serialized

    @classmethod
    def request_cls(cls) -> type[RecordRequest]:
        return RecordRequest

    def as_request_resource(self) -> RecordRequest:
        dumped = self.dump()
        if self.properties:
            dumped["sources"] = [
                RecordSource(source=source_ref, properties=props) for source_ref, props in self.properties.items()
            ]
        return RecordRequest.model_validate(dumped, extra="ignore")
