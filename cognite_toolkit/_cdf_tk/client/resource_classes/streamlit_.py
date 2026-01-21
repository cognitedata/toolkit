from typing import Any, Literal

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
    UpdatableRequestResource,
)

from .filemetadata import FileMetadataRequest
from .identifiers import ExternalId


class StreamlitFile(BaseModelObject):
    """Base class for Streamlit app with common fields."""

    external_id: str
    name: str
    creator: str
    entrypoint: str | None = None
    description: str | None = None
    published: bool = False
    theme: Literal["Light", "Dark"] = "Light"
    thumbnail: str | None = None
    data_set_id: int | None = None
    cognite_toolkit_app_hash: str | None = None


class StreamlitRequest(StreamlitFile, UpdatableRequestResource):
    """Request resource for creating/updating Streamlit apps."""

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)

    def as_file(self) -> FileMetadataRequest:
        metadata: dict[str, str] = {
            "creator": self.creator,
            "name": self.name,
            "published": str(self.published),
            "theme": str(self.theme),
        }
        if self.description:
            metadata["description"] = self.description
        if self.thumbnail:
            metadata["thumbnail"] = self.thumbnail
        if self.cognite_toolkit_app_hash:
            metadata["cdf-toolkit-app-hash"] = self.cognite_toolkit_app_hash
        if self.entrypoint:
            metadata["entrypoint"] = self.entrypoint
        return FileMetadataRequest(
            external_id=self.external_id,
            name=f"{self.name}-source.json",
            data_set_id=self.data_set_id,
            directory="/streamlit-apps/",
            metadata=metadata,
        )

    def dump(self, camel_case: bool = True, exclude_extra: bool = False) -> dict[str, Any]:
        return self.as_file().dump(camel_case=camel_case, exclude_extra=exclude_extra)

    def as_update(self, mode: Literal["patch", "replace"]) -> dict[str, Any]:
        return self.as_file().as_update(mode)


class StreamlitResponse(StreamlitFile, ResponseResource[StreamlitRequest]):
    """Response resource for Streamlit apps."""

    created_time: int
    last_updated_time: int

    def as_request_resource(self) -> StreamlitRequest:
        return StreamlitRequest.model_validate(self.dump(), extra="ignore")
