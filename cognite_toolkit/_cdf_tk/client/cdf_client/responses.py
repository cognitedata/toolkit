from typing import Generic, TypeVar

from pydantic import BaseModel, Field, JsonValue

from cognite_toolkit._cdf_tk.client.resource_classes.graphql_data_model import GraphQLDataModelResponse

T = TypeVar("T", bound=BaseModel)


class ResponseItems(BaseModel, Generic[T]):
    """A page of reference items from a paginated API response.

    Attributes:
        items: The list of reference items in this page.
    """

    items: list[T]


class PagedResponse(BaseModel, Generic[T]):
    items: list[T]
    next_cursor: str | None = Field(None, alias="nextCursor")


class QueryResponse(BaseModel, Generic[T]):
    items: dict[str, list[T]]
    typing: dict[str, JsonValue] | None = None
    next_cursor: dict[str, str] = Field(alias="nextCursor")
    debug: dict[str, JsonValue] | None = None


class GraphQLResponse(BaseModel):
    data: GraphQLDataModelResponse
    errors: list[dict[str, JsonValue]] | None = None


class GraphQLUpsertResponse(BaseModel):
    upsert_graph_ql_dml_version: GraphQLResponse = Field(alias="upsertGraphQlDmlVersion")
