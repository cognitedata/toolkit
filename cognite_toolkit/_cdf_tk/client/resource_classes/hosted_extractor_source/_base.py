import builtins
from typing import Any, ClassVar, Literal

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
    UpdatableRequestResource,
)
from cognite_toolkit._cdf_tk.client._types import Metadata
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId


class SourceRequestDefinition(UpdatableRequestResource):
    container_fields: ClassVar[frozenset[str]] = frozenset({"metadata"})

    type: str
    external_id: str
    name: str | None = None
    description: str | None = None
    metadata: Metadata | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)

    def as_update(self, mode: Literal["patch", "replace"]) -> dict[str, Any]:
        output = super().as_update(mode)
        # Move type from update to top-level, as the API expects it there
        output["update"].pop("type")
        output["type"] = self.type
        return output


class SourceResponseDefinition(BaseModelObject):
    external_id: str
    name: str | None = None
    description: str | None = None
    metadata: Metadata | None = None
    created_time: int
    last_updated_time: int


class UnknownSourceRequest(SourceRequestDefinition):
    type: str


class UnknownSourceResponse(SourceResponseDefinition, ResponseResource[UnknownSourceRequest]):
    type: str

    @classmethod
    def request_cls(cls) -> builtins.type[UnknownSourceRequest]:
        return UnknownSourceRequest

    def as_request_resource(self) -> UnknownSourceRequest:
        return UnknownSourceRequest.model_validate(self.dump(), extra="allow")
