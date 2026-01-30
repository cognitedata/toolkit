"""GraphQL Data Models API for managing CDF GraphQL/DML data models.

This API provides a wrapper around the legacy DML API for managing GraphQL data models.
"""

from collections.abc import Iterable, Sequence
from typing import Any

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.cdf_client.responses import GraphQLUpsertResponse
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsSuccessResponse,
    RequestMessage,
    SuccessResponse,
    ToolkitAPIError,
)
from cognite_toolkit._cdf_tk.client.request_classes.filters import DataModelFilter
from cognite_toolkit._cdf_tk.client.request_classes.graphql import UPSERT_BODY
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import DataModelReference
from cognite_toolkit._cdf_tk.client.resource_classes.graphql_data_model import (
    GraphQLDataModelRequest,
    GraphQLDataModelResponse,
)


class GraphQLDataModelsAPI(CDFResourceAPI[DataModelReference, GraphQLDataModelRequest, GraphQLDataModelResponse]):
    """API for managing CDF GraphQL/DML data models.

    This API uses GraphQL mutations to manage data models with DML (Data Modeling Language).
    """

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
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
        request = RequestMessage(
            endpoint_url=self._make_url("/dml/graphql"),
            method="POST",
            body_content=payload,
        )
        result = self._http_client.request_single_retries(request)
        response = result.get_success_or_raise()
        parsed = GraphQLUpsertResponse.model_validate_json(response.body)
        if errors := parsed.upsert_graph_ql_dml_version.errors:
            raise ToolkitAPIError(f"Failed GraphQL errors: {errors}")
        return parsed

    def create(self, items: Sequence[GraphQLDataModelRequest]) -> list[GraphQLDataModelResponse]:
        """Apply (create or update) GraphQL data models in CDF.

        Args:
            items: List of GraphQLDataModelRequest objects to apply.

        Returns:
            List of applied GraphQLDataModelResponse objects.
        """
        results: list[GraphQLDataModelResponse] = []
        for item in items:
            payload = {"query": UPSERT_BODY, "variables": {"dmCreate": item.dump()}}
            response = self._post_graphql(payload)

            results.append(response.upsert_graph_ql_dml_version.data)

        return results

    def update(self, items: Sequence[GraphQLDataModelRequest]) -> list[GraphQLDataModelResponse]:
        """Update GraphQL data models in CDF.

        Args:
            items: List of GraphQLDataModelRequest objects to update.
        Returns:
            List of updated GraphQLDataModelResponse objects.
        """
        return self.create(items)

    def retrieve(
        self, items: Sequence[DataModelReference], inline_views: bool = False
    ) -> list[GraphQLDataModelResponse]:
        """Retrieve GraphQL data models from CDF.

        Args:
            items: List of DataModelReference objects to retrieve.
            inline_views: Whether to include full view definitions in the response.

        Returns:
            List of retrieved GraphQLDataModelResponse objects.
        """
        return self._request_item_response(items, method="retrieve", extra_body={"inlineViews": inline_views})

    def delete(self, items: Sequence[DataModelReference]) -> None:
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
