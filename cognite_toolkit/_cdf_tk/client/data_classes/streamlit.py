from typing import Literal

from cognite_toolkit._cdf_tk.client.data_classes.base import BaseModelObject, RequestResource, ResponseResource

from .identifiers import ExternalId


class StreamlitBase(BaseModelObject):
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


class StreamlitRequest(StreamlitBase, RequestResource):
    """Request resource for creating/updating Streamlit apps."""

    cognite_toolkit_app_hash: str | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class StreamlitResponse(StreamlitBase, ResponseResource[StreamlitRequest]):
    """Response resource for Streamlit apps."""

    created_time: int
    last_updated_time: int
    cognite_toolkit_app_hash: str | None = None

    def as_request_resource(self) -> StreamlitRequest:
        return StreamlitRequest.model_validate(self.dump(), extra="ignore")
