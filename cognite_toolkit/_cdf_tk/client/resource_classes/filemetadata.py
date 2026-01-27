from typing import Any, ClassVar, Literal

from pydantic import Field, JsonValue

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, ResponseResource, UpdatableRequestResource

from .identifiers import ExternalId
from .instance_api import NodeReference


class FileMetadata(BaseModelObject):
    external_id: str | None = None
    name: str
    directory: str | None = None
    source: str | None = None
    mime_type: str | None = None
    metadata: dict[str, str] | None = None
    asset_ids: list[int] | None = None
    data_set_id: int | None = None
    labels: list[dict[Literal["externalId"], str]] | None = None
    geo_location: JsonValue | None = None
    source_created_time: int | None = None
    source_modified_time: int | None = None
    security_categories: list[int] | None = None

    def as_id(self) -> ExternalId:
        if self.external_id is None:
            raise ValueError("Cannot convert FileMetadataRequest to ExternalId when external_id is None")
        return ExternalId(external_id=self.external_id)


class FileMetadataRequest(FileMetadata, UpdatableRequestResource):
    container_fields: ClassVar[frozenset[str]] = frozenset({"metadata", "labels", "asset_ids", "security_categories"})
    non_nullable_fields: ClassVar[frozenset[str]] = frozenset({"asset_ids", "security_categories"})
    # This field is not part of the request when creating or updating a resource
    # but we added it here for convenience so that it is available when converting
    # from response to request.
    instance_id: NodeReference | None = Field(default=None, exclude=True)

    def as_update(self, mode: Literal["patch", "replace"]) -> dict[str, Any]:
        update = super().as_update(mode)
        # Name cannot be updated.
        update["update"].pop("name", None)
        return update


class FileMetadataResponse(FileMetadata, ResponseResource[FileMetadataRequest]):
    created_time: int
    last_updated_time: int
    uploaded_time: int | None = None
    uploaded: bool
    id: int
    instance_id: NodeReference | None = None
    # This field is required in the upload endpoint response, but not in any other file metadata response
    upload_url: str | None = None

    def as_request_resource(self) -> FileMetadataRequest:
        return FileMetadataRequest.model_validate(self.dump(), extra="ignore")
