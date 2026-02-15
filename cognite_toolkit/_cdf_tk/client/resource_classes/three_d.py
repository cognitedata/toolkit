import sys
from typing import ClassVar, Literal

from pydantic import Field

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    Identifier,
    RequestResource,
    ResponseResource,
    UpdatableRequestResource,
)

from .data_modeling import NodeReference
from .identifiers import InternalId

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class RevisionStatus(BaseModelObject):
    status: Literal["Queued", "Processing", "Done", "Failed"] | None = None
    revision_id: int | None = None
    created_time: int | None = None
    revision_count: int | None = None
    types: list[str] | None = None


class ThreeDModelRequest(RequestResource):
    name: str
    # This field is part of the path request and not the body schema.
    # but is needed for identifier conversion.
    id: int | None = Field(None, exclude=True)

    def as_id(self) -> InternalId:
        if self.id is None:
            raise ValueError("Cannot convert to InternalId when id is None.")
        return InternalId(id=self.id)


class ThreeDModelClassicRequest(ThreeDModelRequest, UpdatableRequestResource):
    container_fields: ClassVar[frozenset[str]] = frozenset({"metadata"})
    data_set_id: int | None = None
    metadata: dict[str, str] | None = None


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


class ThreeDRevisionCamera(BaseModelObject):
    """Camera settings for a 3D revision (target and position)."""

    target: list[float]
    position: list[float]


class ThreeDRevisionClassicRequest(UpdatableRequestResource):
    """Request class for creating/updating a classic 3D revision."""

    container_fields: ClassVar[frozenset[str]] = frozenset({"metadata"})

    published: bool | None = None
    rotation: list[float] | None = None
    scale: list[float] | None = None
    translation: list[float] | None = None
    metadata: dict[str, str] | None = None
    camera: ThreeDRevisionCamera | None = None
    file_id: int
    # This field is used for update/delete and is not part of the create body schema.
    id: int | None = Field(None, exclude=True)
    # model_id is a path parameter, not part of the request body.
    model_id: int = Field(exclude=True)

    def as_id(self) -> InternalId:
        if self.id is None:
            raise ValueError("Cannot convert to InternalId when id is None.")
        return InternalId(id=self.id)


class ThreeDRevisionClassicResponse(ResponseResource[ThreeDRevisionClassicRequest]):
    """Response class for a classic 3D revision."""

    id: int
    file_id: int
    published: bool
    rotation: list[float] | None = None
    scale: list[float] | None = None
    translation: list[float] | None = None
    camera: ThreeDRevisionCamera | None = None
    status: Literal["Queued", "Processing", "Done", "Failed"]
    metadata: dict[str, str] | None = None
    thumbnail_threed_file_id: int | None = None
    thumbnail_url: str | None = Field(None, alias="thumbnailURL")
    asset_mapping_count: int
    created_time: int
    # model_id is a path parameter, not returned in the API response body.
    model_id: int = Field(-1, exclude=True)

    def as_request_resource(self) -> ThreeDRevisionClassicRequest:
        return ThreeDRevisionClassicRequest.model_validate(self.dump(), extra="ignore")


class AssetMappingDMRequest(RequestResource, Identifier):
    node_id: int
    asset_instance_id: NodeReference
    # These fields are part of the path request and not the body schema.
    model_id: int = Field(exclude=True)
    revision_id: int = Field(exclude=True)

    def as_id(self) -> Self:
        return self

    def __str__(self) -> str:
        return f"{self.model_id}_{self.revision_id}_{self.node_id}_{self.asset_instance_id.space}_{self.asset_instance_id.external_id}"


class AssetMappingClassicRequest(RequestResource, Identifier):
    node_id: int
    asset_id: int | None = None
    asset_instance_id: NodeReference | None = None
    # These fields are part of the path request and not the body schema.
    model_id: int = Field(exclude=True)
    revision_id: int = Field(exclude=True)

    def as_id(self) -> Self:
        return self

    def __str__(self) -> str:
        asset_part = (
            f"assetId:{self.asset_id}"
            if self.asset_id is not None
            else f"assetInstance:{self.asset_instance_id.space}_{self.asset_instance_id.external_id}"
            if self.asset_instance_id is not None
            else "noAsset"
        )
        return f"{self.model_id}_{self.revision_id}_{self.node_id}_{asset_part}"


class AssetMappingClassicResponse(ResponseResource[AssetMappingClassicRequest]):
    node_id: int
    asset_id: int | None = None
    asset_instance_id: NodeReference | None = None
    tree_index: int | None = None
    subtree_size: int | None = None
    # These fields are part of the path request and response, but they are included here for convenience.
    model_id: int = Field(-1, exclude=True)
    revision_id: int = Field(-1, exclude=True)

    def as_request_resource(self) -> AssetMappingClassicRequest:
        return AssetMappingClassicRequest.model_validate(
            {**self.dump(), "modelId": self.model_id, "revisionId": self.revision_id}
        )


class AssetMappingDMResponse(ResponseResource[AssetMappingDMRequest]):
    node_id: int
    asset_instance_id: NodeReference
    tree_index: int | None = None
    subtree_size: int | None = None
    # These fields are part of the path request and response, but they are included here for convenience.
    model_id: int = Field(-1, exclude=True)
    revision_id: int = Field(-1, exclude=True)

    def as_request_resource(self) -> AssetMappingDMRequest:
        return AssetMappingDMRequest.model_validate(
            {**self.dump(), "modelId": self.model_id, "revisionId": self.revision_id}
        )
