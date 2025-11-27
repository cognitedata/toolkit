from typing import Literal

from .base import BaseModelObject, RequestResource, ResponseResource


class RevisionStatus(BaseModelObject):
    status: Literal["Queued", "Processing", "Done", "Failed"] | None = None
    revision_id: int | None = None
    created_time: int | None = None
    revision_count: int | None = None
    types: list[str] | None = None


class ThreeDModelRequest(RequestResource):
    name: str
    data_set_id: int | None = None
    metadata: dict[str, str] | None = None


class ThreeDModelResponse(ResponseResource[ThreeDModelRequest]):
    name: str
    id: int
    created_time: int
    data_set_id: int | None = None
    metadata: dict[str, str] | None = None
    space: str | None
    last_revision_info: RevisionStatus | None = None

    def as_request_resource(self) -> ThreeDModelRequest:
        return ThreeDModelRequest._load(self.dump(camel_case=True))
