from datetime import datetime
from typing import ClassVar, Literal

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.client.resource_classes.instance_api import (
    NodeReference,
    TypedNodeIdentifier,
    TypedViewReference,
    WrappedInstanceRequest,
    WrappedInstanceResponse,
)

COGNITE_FILE_VIEW_ID = TypedViewReference(space="cdf_cdm", external_id="CogniteFile", version="v1")


class CogniteFile(BaseModelObject):
    """Base class for CogniteFile containing common properties."""

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


class CogniteFileRequest(WrappedInstanceRequest, CogniteFile):
    """CogniteFile request resource for creating/updating nodes."""

    VIEW_ID: ClassVar[TypedViewReference] = COGNITE_FILE_VIEW_ID
    instance_type: Literal["node"] = "node"
    space: str
    external_id: str

    def as_id(self) -> TypedNodeIdentifier:
        return TypedNodeIdentifier(space=self.space, external_id=self.external_id)


class CogniteFileResponse(WrappedInstanceResponse[CogniteFileRequest], CogniteFile):
    """CogniteFile response resource returned from API."""

    VIEW_ID: ClassVar[TypedViewReference] = COGNITE_FILE_VIEW_ID
    instance_type: Literal["node"] = "node"
    space: str
    external_id: str
    is_uploaded: bool | None = None
    uploaded_time: datetime | None = None

    def as_request_resource(self) -> CogniteFileRequest:
        return CogniteFileRequest.model_validate(
            self.model_dump(mode="json", by_alias=True, exclude_unset=True, exclude={"instance_type"}),
            extra="ignore",
        )
