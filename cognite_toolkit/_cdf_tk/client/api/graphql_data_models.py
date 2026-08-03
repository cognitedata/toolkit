"""GraphQL Data Models API for managing CDF GraphQL/DML data models.

This API provides a wrapper around the legacy DML API for managing GraphQL data models.
"""

import json
from collections.abc import Iterable, Sequence
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, JsonValue

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsSuccessResponse,
    RequestMessage,
    SuccessResponse,
    ToolkitAPIError,
)
from cognite_toolkit._cdf_tk.client.request_classes.filters import DataModelFilter
from cognite_toolkit._cdf_tk.client.request_classes.graphql import UPSERT_BODY
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import DataModelId
from cognite_toolkit._cdf_tk.client.resource_classes.graphql_data_model import (
    GraphQLDataModelRequest,
    GraphQLDataModelResponse,
)
from cognite_toolkit._cdf_tk.utils import humanize_collection


class DMLError(BaseModel):
    model_config = ConfigDict(extra="allow")
    kind: str | None = None
    message: str | None = None
    hint: str | None = None


class UpsertResponseData(BaseModel):
    errors: list[DMLError] | None = None
    result: GraphQLDataModelResponse | None = None


class GraphQLUpsertResponse(BaseModel):
    upsert_graph_ql_dml_version: UpsertResponseData | None = Field(None, alias="upsertGraphQlDmlVersion")


class GraphQLErrors(BaseModel):
    model_config = ConfigDict(extra="allow")
    message: str | None = None
    locations: list[dict[str, int]] | None = None
    extensions: dict[str, JsonValue] | None = None


class GraphQLResponse(BaseModel):
    data: GraphQLUpsertResponse | None = None
    errors: list[GraphQLErrors] | None = None


class GraphQLDataModelsAPI(CDFResourceAPI[GraphQLDataModelResponse]):
    """API for managing CDF GraphQL/DML data models.

    This API uses GraphQL mutations to manage data models with DML (Data Modeling Language).
    """

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/dml/graphql", item_limit=1),
                "retrieve": Endpoint(method="POST", path="/models/datamodels/byids", item_limit=100),
                "delete": Endpoint(method="POST", path="/models/datamodels/delete", item_limit=100),
                "list": Endpoint(method="GET", path="/models/datamodels", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[GraphQLDataModelResponse]:
        return PagedResponse[GraphQLDataModelResponse].model_validate_json(response.body)

    def _post_graphql(self, payload: dict[str, Any]) -> GraphQLUpsertResponse:
        """Execute a GraphQL query against the DML endpoint."""
        endpoint = self._method_endpoint_map["create"]
        request = RequestMessage(
            endpoint_url=self._make_url(endpoint.path),
            method=endpoint.method,
            body_content=payload,
        )
        result = self._http_client.request_single_retries(request)
        response = result.get_success_or_raise(request)
        raw = json.loads(response.body)
        if top_errors := raw.get("errors"):
            messages = [e.get("message", str(e)) for e in top_errors if isinstance(e, dict)]
            raise ToolkitAPIError(f"GraphQL mutation failed: {humanize_collection(messages)}")
        parsed = GraphQLResponse.model_validate(raw)
        if parsed.data is None:
            raise ToolkitAPIError("GraphQL mutation returned no data and no errors.")
        upsert = parsed.data.upsert_graph_ql_dml_version
        if upsert is None:
            raise ToolkitAPIError("GraphQL mutation returned no result and no errors.")
        if upsert.errors:
            messages = [e.message for e in upsert.errors if e.message]
            raise ToolkitAPIError(f"DML validation failed: {humanize_collection(messages)}")
        return parsed.data

    def create(self, items: Sequence[GraphQLDataModelRequest]) -> list[GraphQLDataModelResponse]:
        """Apply (create or update) GraphQL data models in CDF.

        Args:
            items: List of GraphQLDataModelRequest objects to apply.

        Returns:
            List of applied GraphQLDataModelResponse objects.
        """
        results: list[GraphQLDataModelResponse] = []
        for item in items:
            payload = {
                "query": UPSERT_BODY,
                "variables": {"dmCreate": item.dump(exclude_extra=True)},
            }
            response = self._post_graphql(payload)
            upsert = response.upsert_graph_ql_dml_version
            if upsert is None or upsert.result is None:
                raise ToolkitAPIError("GraphQL mutation succeeded but returned no data model.")
            results.append(upsert.result)
        return results

    def retrieve(self, items: Sequence[DataModelId], inline_views: bool = False) -> list[GraphQLDataModelResponse]:
        """Retrieve GraphQL data models from CDF.

        Args:
            items: List of DataModelReference objects to retrieve.
            inline_views: Whether to include full view definitions in the response.

        Returns:
            List of retrieved GraphQLDataModelResponse objects.
        """
        return self._request_item_response(items, method="retrieve", extra_body={"inlineViews": inline_views})

    def delete(self, items: Sequence[DataModelId]) -> None:
        """Delete GraphQL data models from CDF.

        Args:
            items: List of DataModelReference objects to delete.
        """
        self._request_no_response(items, "delete")

    def paginate(
        self,
        filter: DataModelFilter | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[GraphQLDataModelResponse]:
        """Get a page of GraphQL data models from CDF.

        Args:
            filter: DataModelFilter to filter data models.
            limit: Maximum number of data models to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of GraphQLDataModelResponse objects.
        """
        return self._paginate(
            cursor=cursor,
            limit=limit,
            params=filter.dump() if filter else None,
        )

    def iterate(
        self,
        filter: DataModelFilter | None = None,
        limit: int | None = None,
    ) -> Iterable[list[GraphQLDataModelResponse]]:
        """Iterate over all GraphQL data models in CDF.

        Args:
            filter: DataModelFilter to filter data models.
            limit: Maximum total number of data models to return.

        Returns:
            Iterable of lists of GraphQLDataModelResponse objects.
        """
        return self._iterate(
            limit=limit,
            params=filter.dump() if filter else None,
        )

    def list(
        self,
        filter: DataModelFilter | None = None,
        limit: int | None = None,
    ) -> list[GraphQLDataModelResponse]:
        """List all GraphQL data models in CDF.

        Args:
            filter: DataModelFilter to filter data models.
            limit: Maximum total number of data models to return.

        Returns:
            List of GraphQLDataModelResponse objects.
        """
        return self._list(limit=limit, params=filter.dump() if filter else None)
