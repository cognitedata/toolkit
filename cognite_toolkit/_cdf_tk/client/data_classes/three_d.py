from collections.abc import Hashable
from typing import Literal

from pydantic import Field

from .base import BaseModelObject, RequestResource, ResponseResource


class NodeReference(BaseModelObject):
    space: str
    external_id: str


class RevisionStatus(BaseModelObject):
    status: Literal["Queued", "Processing", "Done", "Failed"] | None = None
    revision_id: int | None = None
    created_time: int | None = None
    revision_count: int | None = None
    types: list[str] | None = None


class ThreeDModelRequest(RequestResource):
    name: str

    def as_id(self) -> str:
        return self.name


class ThreeDModelClassicRequest(ThreeDModelRequest):
    data_set_id: int | None = None
    metadata: dict[str, str] | None = None

    def as_id(self) -> str:
        return self.name


class ThreeDModelDMSRequest(ThreeDModelRequest):
    space: str
    type: Literal["CAD", "PointCloud", "Image360"]
    thumbnail_reference: NodeReference | None = None


class ThreeDModelResponse(ResponseResource[ThreeDModelRequest]):
    name: str
    id: int
    created_time: int
    data_set_id: int | None = None
    metadata: dict[str, str] | None = None
    space: str | None = None
    last_revision_info: RevisionStatus | None = None

    def as_request_resource(self) -> ThreeDModelRequest:
        if self.space is None:
            return ThreeDModelClassicRequest._load(self.dump())
        else:
            return ThreeDModelDMSRequest._load(self.dump())


class AssetMappingDMRequest(RequestResource):
    node_id: int
    asset_instance_id: NodeReference
    # These fields are part of the path request and not the body schema.
    model_id: int = Field(exclude=True)
    revision_id: int = Field(exclude=True)

    def as_id(self) -> Hashable:
        return (
            self.model_id,
            self.revision_id,
            self.node_id,
            self.asset_instance_id.space,
            self.asset_instance_id.external_id,
        )


class AssetMappingClassicRequest(RequestResource):
    node_id: int
    asset_id: int | None = None
    asset_instance_id: NodeReference | None = None
    # These fields are part of the path request and not the body schema.
    model_id: int = Field(exclude=True)
    revision_id: int = Field(exclude=True)

    def as_id(self) -> Hashable:
        if self.asset_id:
            return self.model_id, self.revision_id, self.node_id, self.asset_id
        elif self.asset_instance_id:
            return (
                self.model_id,
                self.revision_id,
                self.node_id,
                self.asset_instance_id.space,
                self.asset_instance_id.external_id,
            )
        else:
            raise AttributeError("asset_id or asset_instance_id is required")


class AssetMappingResponse(ResponseResource[AssetMappingClassicRequest]):
    node_id: int
    asset_id: int | None = None
    asset_instance_id: NodeReference | None = None
    tree_index: int | None = None
    subtree_size: int | None = None
    model_id: int = Field(exclude=True)
    revision_id: int = Field(exclude=True)

    def as_request_resource(self) -> AssetMappingClassicRequest:
        return AssetMappingClassicRequest.model_validate(
            {**self.dump(), "modelId": self.model_id, "revisionId": self.revision_id}
        )
