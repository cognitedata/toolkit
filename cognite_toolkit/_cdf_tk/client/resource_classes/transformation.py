from typing import Annotated, Any, ClassVar, Literal

from pydantic import Field, JsonValue, field_validator

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
    UpdatableRequestResource,
)

from .identifiers import ExternalId


class NonceCredentials(BaseModelObject):
    session_id: int
    nonce: str
    cdf_project_name: str
    client_id: str | None = None


class DestinationDefinition(BaseModelObject):
    type: str


class DataModelInfo(BaseModelObject):
    space: str
    external_id: str
    version: str
    destination_type: str
    destination_relationship_from_type: str | None = None


class ViewInfo(BaseModelObject):
    space: str
    external_id: str
    version: str


class EdgeType(BaseModelObject):
    space: str
    external_id: str


class AssetCentricDataSource(DestinationDefinition):
    type: Literal[
        "assets",
        "events",
        "asset_hierarchy",
        "datapoints",
        "string_datapoints",
        "timeseries",
        "sequences",
        "files",
        "labels",
        "relationships",
        "data_sets",
    ]


class DataModelSource(DestinationDefinition):
    type: Literal["instances"] = "instances"
    data_model: DataModelInfo
    instance_space: str | None = None


class ViewDataSource(DestinationDefinition):
    type: Literal["nodes", "edges"]
    view: ViewInfo
    edge_type: EdgeType | None = None
    instance_space: str | None = None


class RawDataSource(DestinationDefinition):
    type: Literal["raw"] = "raw"
    database: str
    table: str


class SequenceRowDataSource(DestinationDefinition):
    type: Literal["sequence_rows"] = "sequence_rows"
    external_id: str


Destination = Annotated[
    AssetCentricDataSource | DataModelSource | ViewDataSource | RawDataSource | SequenceRowDataSource,
    Field(discriminator="type"),
]


class BlockedInfo(BaseModelObject):
    reason: str
    created_time: int


class SessionInfo(BaseModelObject):
    client_id: str | None = None
    session_id: int
    project_name: str


class Transformation(BaseModelObject):
    external_id: str
    name: str
    ignore_null_fields: bool
    data_set_id: int | None = None
    tags: list[str] | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class TransformationRequest(Transformation, UpdatableRequestResource):
    container_fields: ClassVar[frozenset[str]] = frozenset({"tags"})
    non_nullable_fields: ClassVar[frozenset[str]] = frozenset({"is_public", "query", "destination", "conflict_mode"})
    query: str | None = None
    conflict_mode: str | None = None
    destination: Destination | None = None
    source_nonce: NonceCredentials | None = None
    destination_nonce: NonceCredentials | None = None
    is_public: bool | None = None


class TransformationResponse(Transformation, ResponseResource[TransformationRequest]):
    id: int
    created_time: int
    last_updated_time: int
    query: str
    is_public: bool
    conflict_mode: str
    destination: Destination
    blocked: BlockedInfo | None = None
    # In the API spec, owner is documented as a string, but the response is actually {"user": '<uuid>'}
    owner: str | dict[str, str]
    owner_is_current_user: bool
    has_source_oidc_credentials: bool
    has_destination_oidc_credentials: bool
    source_session: SessionInfo | None = None
    destination_session: SessionInfo | None = None
    last_finished_job: dict[str, JsonValue] | None = None
    running_job: dict[str, JsonValue] | None = None
    schedule: dict[str, JsonValue] | None = None

    def as_request_resource(self) -> TransformationRequest:
        return TransformationRequest.model_validate(self.dump(), extra="ignore")


class ColumnType(BaseModelObject):
    type: str


class Column(BaseModelObject):
    name: str
    sql_type: str
    type: str | ColumnType
    nullable: bool


class QueryResponse(BaseModelObject):
    schema_: list[Column] = Field(..., alias="schema")
    results: list[dict[str, str | int | float | bool | None]]

    @field_validator("schema_", "results", mode="before")
    def _remove_items_wrapper(cls, value: Any) -> Any:
        if isinstance(value, dict) and "items" in value:
            return value["items"]
        return value
