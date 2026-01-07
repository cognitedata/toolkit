from typing import ClassVar, Literal

from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client.data_classes.base import RequestUpdateable, ResponseResource

from .identifiers import ExternalId
from .instance_api import NodeReference


class FileMetadataRequest(RequestUpdateable):
    container_fields: ClassVar[frozenset[str]] = frozenset({"metadata", "labels", "asset_ids", "security_categories"})
    non_nullable_fields: ClassVar[frozenset[str]] = frozenset({"asset_ids", "security_categories"})
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


class FileMetadataResponse(ResponseResource[FileMetadataRequest]):
    created_time: int
    last_updated_time: int
    uploaded_time: int | None = None
    uploaded: bool
    id: int
    external_id: str | None = None
    name: str
    directory: str | None = None
    instance_id: NodeReference | None = None
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

    def as_request_resource(self) -> FileMetadataRequest:
        return FileMetadataRequest.model_validate(self.dump(), extra="ignore")
