from cognite_toolkit._cdf_tk.client.data_classes.base import (
    BaseModelObject,
    Identifier,
    RequestResource,
    ResponseResource,
)


class DataModelId(Identifier):
    """Identifier for a data model."""

    space: str
    external_id: str
    version: str

    def __str__(self) -> str:
        return f"space='{self.space}', externalId='{self.external_id}', version='{self.version}'"


class ViewId(BaseModelObject):
    """View identifier returned in GraphQL data model responses."""

    space: str
    external_id: str
    version: str | None = None


class GraphQLDataModelBase(BaseModelObject):
    """Base class for GraphQL data model with common fields."""

    space: str
    external_id: str
    version: str
    name: str | None = None
    description: str | None = None


class GraphQLDataModelRequest(GraphQLDataModelBase, RequestResource):
    """Request resource for creating/updating GraphQL data models."""

    previous_version: str | None = None
    dml: str | None = None
    preserve_dml: bool | None = None

    def as_id(self) -> DataModelId:
        return DataModelId(space=self.space, external_id=self.external_id, version=self.version)


class GraphQLDataModelResponse(GraphQLDataModelBase, ResponseResource[GraphQLDataModelRequest]):
    """Response resource for GraphQL data models."""

    is_global: bool
    created_time: int
    last_updated_time: int
    views: list[ViewId] | None = None

    def as_request_resource(self) -> GraphQLDataModelRequest:
        return GraphQLDataModelRequest.model_validate(self.dump(), extra="ignore")
