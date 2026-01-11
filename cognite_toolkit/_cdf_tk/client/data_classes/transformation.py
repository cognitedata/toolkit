from cognite_toolkit._cdf_tk.client.data_classes.base import (
    BaseModelObject,
    RequestResource,
    ResponseResource,
)

from .identifiers import ExternalId


class DataModelInfo(BaseModelObject):
    space: str | None = None
    external_id: str | None = None
    version: str | None = None
    destination_type: str | None = None
    destination_relationship_from_type: str | None = None


class ViewInfo(BaseModelObject):
    space: str | None = None
    external_id: str | None = None
    version: str | None = None


class EdgeType(BaseModelObject):
    space: str | None = None
    external_id: str | None = None


class Destination(BaseModelObject):
    type: str | None = None
    database: str | None = None
    table: str | None = None
    data_model: DataModelInfo | None = None
    view: ViewInfo | None = None
    edge_type: EdgeType | None = None
    instance_space: str | None = None


class Transformation(BaseModelObject):
    external_id: str
    name: str | None = None
    query: str | None = None
    destination: Destination | None = None
    conflict_mode: str | None = None
    is_public: bool | None = None
    ignore_null_fields: bool | None = None
    data_set_id: int | None = None
    tags: list[str] | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class TransformationRequest(Transformation, RequestResource): ...


class TransformationResponse(Transformation, ResponseResource[TransformationRequest]):
    id: int
    created_time: int
    last_updated_time: int
    owner_is_current_user: bool | None = None
    has_source_oidc_credentials: bool | None = None
    has_destination_oidc_credentials: bool | None = None

    def as_request_resource(self) -> TransformationRequest:
        return TransformationRequest.model_validate(self.dump(), extra="ignore")
