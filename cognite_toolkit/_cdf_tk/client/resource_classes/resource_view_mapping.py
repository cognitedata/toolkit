from typing import ClassVar, Literal

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
)

from .data_modeling import ViewReference
from .identifiers import ExternalId
from .instance_api import TypedViewReference, WrappedInstanceRequest, WrappedInstanceResponse

RESOURCE_MAPPING_VIEW_ID = TypedViewReference(
    space="cognite_migration", external_id="ResourceViewMapping", version="v1"
)


class ResourceViewMapping(BaseModelObject):
    resource_type: str
    view_id: ViewReference
    property_mapping: dict[str, str]


class ResourceViewMappingRequest(WrappedInstanceRequest, ResourceViewMapping):
    VIEW_ID: ClassVar[TypedViewReference] = RESOURCE_MAPPING_VIEW_ID
    space: Literal["cognite_migration"] = "cognite_migration"
    instance_type: Literal["node"] = "node"

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class ResourceViewMappingResponse(WrappedInstanceResponse[ResourceViewMappingRequest], ResourceViewMapping):
    VIEW_ID: ClassVar[TypedViewReference] = RESOURCE_MAPPING_VIEW_ID
    space: Literal["cognite_migration"] = "cognite_migration"
    instance_type: Literal["node"] = "node"

    def as_request_resource(self) -> ResourceViewMappingRequest:
        return ResourceViewMappingRequest.model_validate(self.dump(), extra="ignore")
