"""GraphQL Data Models API for managing CDF GraphQL/DML data models.

This API provides a wrapper around the legacy DML API for managing GraphQL data models.
"""

import textwrap
from collections.abc import Iterable, Sequence
from typing import Any

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsSuccessResponse2,
    RequestMessage2,
    SuccessResponse2,
    ToolkitAPIError,
)
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
                "retrieve": Endpoint(
                    method="POST", path="/models/datamodels/byids", item_limit=100, concurrency_max_workers=1
                ),
                "delete": Endpoint(
                    method="POST", path="/models/datamodels/delete", item_limit=100, concurrency_max_workers=1
                ),
                "list": Endpoint(method="GET", path="/models/datamodels", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse2 | ItemsSuccessResponse2
    ) -> PagedResponse[GraphQLDataModelResponse]:
        return PagedResponse[GraphQLDataModelResponse].model_validate_json(response.body)

    def _post_graphql(self, query_name: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Execute a GraphQL query against the DML endpoint."""
        request = RequestMessage2(
            endpoint_url=self._make_url("/dml/graphql"),
            method="POST",
            body_content=payload,
        )
        result = self._http_client.request_single_retries(request)
        response = result.get_success_or_raise()
        res = response.body_json

        # Errors can be passed both at top level and nested in the response
        errors = res.get("errors", []) + ((res.get("data", {}).get(query_name) or {}).get("errors") or [])
        if errors:
            error_messages = [error.get("message", str(error)) for error in errors]
            raise ToolkitAPIError(f"GraphQL errors: {'; '.join(error_messages)}", status_code=400)

        return res["data"]

    def apply(self, items: Sequence[GraphQLDataModelRequest]) -> list[GraphQLDataModelResponse]:
        """Apply (create or update) GraphQL data models in CDF.

        Args:
            items: List of GraphQLDataModelRequest objects to apply.

        Returns:
            List of applied GraphQLDataModelResponse objects.
        """
        results: list[GraphQLDataModelResponse] = []

        graphql_body = """
            mutation UpsertGraphQlDmlVersion($dmCreate: GraphQlDmlVersionUpsert!) {
                upsertGraphQlDmlVersion(graphQlDmlVersion: $dmCreate) {
                    errors {
                        kind
                        message
                        hint
                        location {
                            start {
                                line
                                column
                            }
                        }
                    }
                    result {
                        space
                        externalId
                        version
                        name
                        description
                        graphQlDml
                        createdTime
                        lastUpdatedTime
                    }
                }
            }
        """

        for item in items:
            payload = {
                "query": textwrap.dedent(graphql_body),
                "variables": {
                    "dmCreate": {
                        "space": item.space,
                        "externalId": item.external_id,
                        "version": item.version,
                        "previousVersion": item.previous_version,
                        "graphQlDml": item.dml,
                        "name": item.name,
                        "description": item.description,
                        "preserveDml": item.preserve_dml,
                    }
                },
            }

            query_name = "upsertGraphQlDmlVersion"
            res = self._post_graphql(query_name, payload)
            result_data = res[query_name]["result"]

            # Convert the result to a GraphQLDataModelResponse
            response = GraphQLDataModelResponse(
                space=result_data["space"],
                external_id=result_data["externalId"],
                version=result_data["version"],
                name=result_data.get("name"),
                description=result_data.get("description"),
                is_global=False,  # GraphQL data models are not global
                created_time=result_data["createdTime"],
                last_updated_time=result_data["lastUpdatedTime"],
            )
            results.append(response)

        return results

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
        space: str | None = None,
        include_global: bool = False,
        all_versions: bool = False,
        inline_views: bool = False,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[GraphQLDataModelResponse]:
        """Get a page of GraphQL data models from CDF.

        Args:
            space: Filter by space.
            include_global: Whether to include global data models.
            all_versions: Whether to include all versions.
            inline_views: Whether to include full view definitions.
            limit: Maximum number of data models to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of GraphQLDataModelResponse objects.
        """
        params = {
            "includeGlobal": include_global,
            "allVersions": all_versions,
            "inlineViews": inline_views,
        }
        if space is not None:
            params["space"] = space
        return self._paginate(
            cursor=cursor,
            limit=limit,
            params=params,
        )

    def iterate(
        self,
        space: str | None = None,
        include_global: bool = False,
        all_versions: bool = False,
        inline_views: bool = False,
        limit: int | None = None,
    ) -> Iterable[list[GraphQLDataModelResponse]]:
        """Iterate over all GraphQL data models in CDF.

        Args:
            space: Filter by space.
            include_global: Whether to include global data models.
            all_versions: Whether to include all versions.
            inline_views: Whether to include full view definitions.
            limit: Maximum total number of data models to return.

        Returns:
            Iterable of lists of GraphQLDataModelResponse objects.
        """
        params = {
            "includeGlobal": include_global,
            "allVersions": all_versions,
            "inlineViews": inline_views,
        }
        if space is not None:
            params["space"] = space
        return self._iterate(
            limit=limit,
            params=params,
        )

    def list(
        self,
        space: str | None = None,
        include_global: bool = False,
        all_versions: bool = False,
        inline_views: bool = False,
        limit: int | None = None,
    ) -> list[GraphQLDataModelResponse]:
        """List all GraphQL data models in CDF.

        Args:
            space: Filter by space.
            include_global: Whether to include global data models.
            all_versions: Whether to include all versions.
            inline_views: Whether to include full view definitions.
            limit: Maximum total number of data models to return.

        Returns:
            List of GraphQLDataModelResponse objects.
        """
        params = {
            "includeGlobal": include_global,
            "allVersions": all_versions,
            "inlineViews": inline_views,
        }
        if space is not None:
            params["space"] = space
        return self._list(limit=limit, params=params)
