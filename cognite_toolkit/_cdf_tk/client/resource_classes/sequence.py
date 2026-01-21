from typing import ClassVar, Literal

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
    UpdatableRequestResource,
)

from .identifiers import ExternalId


class SequenceColumn(BaseModelObject):
    external_id: str
    name: str | None = None
    description: str | None = None
    metadata: dict[str, str] | None = None
    value_type: Literal["STRING", "DOUBLE", "LONG"] | None = None


class Sequence(BaseModelObject):
    external_id: str | None = None
    name: str | None = None
    description: str | None = None
    asset_id: int | None = None
    data_set_id: int | None = None
    metadata: dict[str, str] | None = None
    columns: list[SequenceColumn]

    def as_id(self) -> ExternalId:
        if self.external_id is None:
            raise ValueError("Cannot convert Sequence to ExternalId when external_id is None")
        return ExternalId(external_id=self.external_id)


class SequenceRequest(Sequence, UpdatableRequestResource):
    container_fields: ClassVar[frozenset[str]] = frozenset({"metadata", "columns"})


class SequenceResponse(Sequence, ResponseResource[SequenceRequest]):
    id: int
    created_time: int
    last_updated_time: int

    def as_request_resource(self) -> SequenceRequest:
        return SequenceRequest.model_validate(self.dump(), extra="ignore")
