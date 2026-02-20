from typing import Any, Literal

from pydantic import Field, model_validator

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
    UpdatableRequestResource,
)

from .identifiers import ExternalId

STREAMLIT_DIRECTORY = "/streamlit-apps/"


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
    cognite_toolkit_app_hash: str | None = Field(None, alias="cdf-toolkit-app-hash")


class StreamlitRequest(StreamlitFile, UpdatableRequestResource):
    """Request resource for creating/updating Streamlit apps."""

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)

    def dump(
        self, camel_case: bool = True, exclude_extra: bool = False, context: Literal["api", "toolkit"] = "api"
    ) -> dict[str, Any]:
        if context == "toolkit":
            return super().dump(camel_case=camel_case, exclude_extra=exclude_extra)
        metadata = self._as_metadata()
        # Returning the file metadata structure expected by the API.
        return {
            "externalId" if camel_case else "external_id": self.external_id,
            "name": f"{self.name}-source.json",
            "dataSetId" if camel_case else "data_set_id": self.data_set_id,
            "directory": STREAMLIT_DIRECTORY,
            "metadata": metadata,
        }

    def _as_metadata(self) -> dict[str, str]:
        metadata: dict[str, str] = {
            "creator": self.creator,
            "name": self.name,
            "published": str(self.published).lower(),
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
        return metadata

    def as_update(self, mode: Literal["patch", "replace"]) -> dict[str, Any]:
        update_data: dict[str, Any] = {
            "metadata": {"set": self._as_metadata()},
            "directory": {"set": STREAMLIT_DIRECTORY},
        }
        if self.data_set_id is not None:
            update_data["dataSetId"] = {"set": self.data_set_id}
        elif mode == "replace":
            update_data["dataSetId"] = {"setNull": True}

        return {
            "externalId": self.external_id,
            "update": update_data,
        }


class StreamlitResponse(StreamlitFile, ResponseResource[StreamlitRequest]):
    """Response resource for Streamlit apps."""

    id: int
    created_time: int
    last_updated_time: int

    def as_request_resource(self) -> StreamlitRequest:
        return StreamlitRequest.model_validate(self.dump(), extra="ignore")

    @model_validator(mode="before")
    def move_metadata(cls, values: dict[str, Any]) -> dict[str, Any]:
        if "metadata" not in values:
            return values
            # Move metadata fields to top level if they exist
        values_copy = values.copy()
        metadata = values_copy.pop("metadata")
        values_copy.update(metadata)
        return values_copy
