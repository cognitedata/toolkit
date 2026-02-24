from typing import Literal

from pydantic import Field, JsonValue, field_serializer

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject

from ._instance import InstanceResponse
from ._references import ViewReference


class QuerySortSpec(BaseModelObject):
    """Sorting specification for query results."""

    property: list[str]
    direction: Literal["ascending", "descending"] | None = None
    nulls_first: bool | None = None


class QueryUnitReference(BaseModelObject):
    """Reference to a unit for unit conversion in query results."""

    external_id: str


class QueryUnitSystemReference(BaseModelObject):
    unit_system_name: str


class QueryTargetUnit(BaseModelObject):
    """Target unit conversion specification for a property."""

    property: str
    unit: QueryUnitReference | QueryUnitSystemReference


class QueryThrough(BaseModelObject):
    """Traversal specification through a direct relation property on a view."""

    source: ViewReference
    identifier: str


class QueryExpression(BaseModelObject):
    sort: list[QuerySortSpec] | None = None
    limit: int | None = None


class QueryTableExpression(BaseModelObject):
    from_: str | None = Field(None, alias="from")
    chain_to: Literal["source", "destination"] | None = None
    direction: Literal["outwards", "inwards"] | None = None


class QueryNodeTableExpression(QueryTableExpression):
    """Table expression defining how to select nodes in a query result set."""

    through: QueryThrough | None = None
    filter: dict[str, JsonValue] | None = None


class QueryEdgeTableExpression(QueryTableExpression):
    """Table expression defining how to select edges in a query result set."""

    max_distance: int | None = None
    filter: dict[str, JsonValue] | None = None
    node_filter: dict[str, JsonValue] | None = None
    termination_filter: dict[str, JsonValue] | None = None
    limit_each: int | None = None


class QueryNodeExpression(QueryExpression):
    """A named result set expression in the 'with' clause of a query.

    Must specify exactly one of ``nodes`` or ``edges``.
    """

    nodes: QueryNodeTableExpression


class QueryEdgeExpression(QueryExpression):
    """A named result set expression in the 'with' clause of a query."""

    post_sort: list[QuerySortSpec] | None = None
    edges: QueryEdgeTableExpression


class QuerySelectSource(BaseModelObject):
    """Source selector specifying which view properties to return."""

    source: ViewReference
    properties: list[str]
    target_units: list[QueryTargetUnit] | None = None

    @field_serializer("source")
    def include_type(self, source: ViewReference) -> dict[str, str]:
        return {**source.dump(), "type": "view"}


class QuerySelect(BaseModelObject):
    """Select clause for a result set expression, specifying which properties to return."""

    sources: list[QuerySelectSource] | None = None
    sort: list[QuerySortSpec] | None = None
    limit: int | None = None


class QueryDebugParameters(BaseModelObject):
    """Debug parameters for the query endpoint."""

    emit_results: bool | None = None
    timeout: int | None = None
    profile: bool | None = None


class QueryRequest(BaseModelObject):
    """Request body for the ``POST /models/instances/query`` endpoint.

    See `API docs <https://api-docs.cognite.com/20230101/tag/Instances/operation/queryContent>`_.
    """

    with_: dict[str, QueryNodeExpression | QueryEdgeExpression] = Field(alias="with")
    cursors: dict[str, str] | None = None
    select: dict[str, QuerySelect]
    parameters: dict[str, JsonValue] | None = None
    include_typing: bool | None = None
    debug: QueryDebugParameters | None = None


class QueryResponse(BaseModelObject):
    """Response from the ``POST /models/instances/query`` endpoint."""

    items: dict[str, list[InstanceResponse]]
    # For now we do not care about the typing and debug structures in the response.
    typing: dict[str, JsonValue] | None = None
    next_cursor: dict[str, str | None]
    debug: dict[str, JsonValue] | None = None
