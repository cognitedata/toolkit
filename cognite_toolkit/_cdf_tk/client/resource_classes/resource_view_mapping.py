from typing import ClassVar, Literal

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
)
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling.instance_api import (
    WrappedInstanceRequest,
    WrappedInstanceResponse,
)

from .data_modeling import NodeReference, ViewReference

RESOURCE_VIEW_MAPPING_SPACE: Literal["cognite_migration"] = "cognite_migration"
RESOURCE_MAPPING_VIEW_ID = ViewReference(
    space=RESOURCE_VIEW_MAPPING_SPACE, external_id="ResourceViewMapping", version="v1"
)


class ResourceViewMapping(BaseModelObject):
    resource_type: str
    view_id: ViewReference
    property_mapping: dict[str, str]


class ResourceViewMappingRequest(WrappedInstanceRequest, ResourceViewMapping):
    VIEW_ID: ClassVar[ViewReference] = RESOURCE_MAPPING_VIEW_ID
    space: Literal["cognite_migration"] = RESOURCE_VIEW_MAPPING_SPACE
    instance_type: Literal["node"] = "node"

    def as_id(self) -> NodeReference:
        return NodeReference(space=self.space, external_id=self.external_id)


class ResourceViewMappingResponse(WrappedInstanceResponse[ResourceViewMappingRequest], ResourceViewMapping):
    VIEW_ID: ClassVar[ViewReference] = RESOURCE_MAPPING_VIEW_ID
    space: Literal["cognite_migration"] = RESOURCE_VIEW_MAPPING_SPACE
    instance_type: Literal["node"] = "node"

    @classmethod
    def request_cls(cls) -> type[ResourceViewMappingRequest]:
        return ResourceViewMappingRequest

    def as_request_resource(self) -> ResourceViewMappingRequest:
        return ResourceViewMappingRequest.model_validate(self.dump(context="toolkit"), extra="ignore")
