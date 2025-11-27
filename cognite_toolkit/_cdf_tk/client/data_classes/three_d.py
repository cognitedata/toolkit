from typing import Literal

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


class ThreeDModelClassicRequest(ThreeDModelRequest):
    data_set_id: int | None = None
    metadata: dict[str, str] | None = None


class ThreeModelDMSRequest(ThreeDModelRequest):
    space: str
    type: Literal["CAD", "PointCloud", "Image360"]
    thumbnail_reference: NodeReference | None = None


class ThreeDModelResponse(ResponseResource[ThreeDModelRequest]):
    name: str
    id: int
    created_time: int
    data_set_id: int | None = None
    metadata: dict[str, str] | None = None
    space: str | None
    last_revision_info: RevisionStatus | None = None

    def as_request_resource(self) -> ThreeDModelRequest:
        if self.space is None:
            return ThreeDModelClassicRequest._load(self.dump())
        else:
            return ThreeModelDMSRequest._load(self.dump())
