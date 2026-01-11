from typing import Any

from cognite_toolkit._cdf_tk.client.data_classes.base import (
    BaseModelObject,
    RequestResource,
    ResponseResource,
)

from .identifiers import ExternalId


class Transformation(BaseModelObject):
    external_id: str
    name: str | None = None
    query: str | None = None
    destination: dict[str, Any] | None = None
    conflict_mode: str | None = None
    is_public: bool | None = None
    ignore_null_fields: bool | None = None
    data_set_id: int | None = None
    tags: list[str] | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class TransformationRequest(Transformation, RequestResource):
    pass


class TransformationResponse(Transformation, ResponseResource[TransformationRequest]):
    id: int
    created_time: int
    last_updated_time: int
    owner_is_current_user: bool | None = None
    has_source_oidc_credentials: bool | None = None
    has_destination_oidc_credentials: bool | None = None

    def as_request_resource(self) -> TransformationRequest:
        return TransformationRequest.model_validate(self.dump(), extra="ignore")
