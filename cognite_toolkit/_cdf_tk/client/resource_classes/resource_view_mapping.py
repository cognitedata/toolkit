from typing import ClassVar, Literal

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    RequestResource,
    ResponseResource,
)

from .data_modeling import ViewReference
from .identifiers import ExternalId


class ResourceViewMapping(BaseModelObject):
    space: ClassVar[str] = "cognite_migration"
    view_ref: ClassVar[ViewReference] = ViewReference(
        space="cognite_migration", external_id="ResourceViewMapping", version="v1"
    )
    instance_type: Literal["node"] = "node"
    external_id: str
    resource_type: str
    view_id: ViewReference
    property_mapping: dict[str, str]

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class ResourceViewMappingRequest(ResourceViewMapping, RequestResource):
    existing_version: int | None = None


class ResourceViewMappingResponse(ResourceViewMapping, ResponseResource[ResourceViewMappingRequest]):
    version: int
    created_time: int
    last_updated_time: int

    def as_request_resource(self) -> ResourceViewMappingRequest:
        return ResourceViewMappingRequest.model_validate(self.dump(), extra="ignore")
