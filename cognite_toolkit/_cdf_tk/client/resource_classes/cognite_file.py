from datetime import datetime

from cognite_toolkit._cdf_tk.client.resource_classes.base import (
    BaseModelObject,
    RequestResource,
    ResponseResource,
)

from .identifiers import ExternalId
from .instance_api import NodeReference


class CogniteFile(BaseModelObject):
    space: str
    external_id: str
    name: str | None = None
    description: str | None = None
    tags: list[str] | None = None
    aliases: list[str] | None = None
    source_id: str | None = None
    source_context: str | None = None
    source: NodeReference | None = None
    source_created_time: datetime | None = None
    source_updated_time: datetime | None = None
    source_created_user: str | None = None
    source_updated_user: str | None = None
    assets: list[NodeReference] | None = None
    mime_type: str | None = None
    directory: str | None = None
    category: NodeReference | None = None
    type: NodeReference | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class CogniteFileRequest(CogniteFile, RequestResource):
    existing_version: int | None = None


class CogniteFileResponse(CogniteFile, ResponseResource[CogniteFileRequest]):
    version: int
    created_time: int
    last_updated_time: int
    is_uploaded: bool | None = None
    uploaded_time: datetime | None = None
    deleted_time: int | None = None

    def as_request_resource(self) -> CogniteFileRequest:
        return CogniteFileRequest.model_validate(self.dump(), extra="ignore")
