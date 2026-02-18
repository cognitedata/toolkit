from datetime import datetime

from pydantic import Field

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    RequestResource,
    ResponseResource,
)

from .data_modeling import DataModelReference, ViewReference


class GraphQLDataModel(BaseModelObject):
    """Base class for GraphQL data model with common fields."""

    space: str
    external_id: str
    version: str
    name: str | None = None
    description: str | None = None
    graph_ql_dml: str | None = None

    def as_id(self) -> DataModelReference:
        return DataModelReference(space=self.space, external_id=self.external_id, version=self.version)


class GraphQLDataModelRequest(GraphQLDataModel, RequestResource):
    """Request resource for creating/updating GraphQL data models."""

    previous_version: str | None = None
    preserve_dml: bool | None = None
    # Used in the loading process, but not part of the API payload.
    graphql_file: str | None = Field(None, exclude=True)


class GraphQLDataModelResponse(GraphQLDataModel, ResponseResource[GraphQLDataModelRequest]):
    """Response resource for GraphQL data models."""

    created_time: datetime
    last_updated_time: datetime
    views: list[ViewReference] | None = None

    def as_request_resource(self) -> GraphQLDataModelRequest:
        return GraphQLDataModelRequest.model_validate(self.dump(), extra="ignore")
