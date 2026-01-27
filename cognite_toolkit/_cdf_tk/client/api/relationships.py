"""Relationships API for managing CDF relationships.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Relationships/operation/createRelationships
"""

from collections.abc import Iterable, Sequence
from typing import Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.relationship import RelationshipRequest, RelationshipResponse


class RelationshipsAPI(CDFResourceAPI[ExternalId, RelationshipRequest, RelationshipResponse]):
    """API for managing CDF relationships."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/relationships", item_limit=1000),
                "retrieve": Endpoint(method="POST", path="/relationships/byids", item_limit=1000),
                "update": Endpoint(method="POST", path="/relationships/update", item_limit=1000),
                "delete": Endpoint(method="POST", path="/relationships/delete", item_limit=1000),
                "list": Endpoint(method="POST", path="/relationships/list", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[RelationshipResponse]:
        return PagedResponse[RelationshipResponse].model_validate_json(response.body)

    def create(self, items: Sequence[RelationshipRequest]) -> list[RelationshipResponse]:
        """Create relationships in CDF.

        Args:
            items: List of RelationshipRequest objects to create.

        Returns:
            List of created RelationshipResponse objects.
        """
        return self._request_item_response(items, "create")

    def retrieve(
        self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False, fetch_resources: bool = False
    ) -> list[RelationshipResponse]:
        """Retrieve relationships from CDF.

        Args:
            items: List of ExternalId objects to retrieve.
            ignore_unknown_ids: Whether to ignore unknown IDs.
            fetch_resources: If true, will try to fetch the resources referred to in the relationship,
                based on the users access rights. Will silently fail to attach the resources
                if the user lacks access to some of them.

        Returns:
            List of retrieved RelationshipResponse objects.
        """
        return self._request_item_response(
            items,
            method="retrieve",
            extra_body={"ignoreUnknownIds": ignore_unknown_ids, "fetchResources": fetch_resources},
        )

    def update(
        self, items: Sequence[RelationshipRequest], mode: Literal["patch", "replace"] = "replace"
    ) -> list[RelationshipResponse]:
        """Update relationships in CDF.

        Args:
            items: List of RelationshipRequest objects to update.
            mode: Update mode, either "patch" or "replace".

        Returns:
            List of updated RelationshipResponse objects.
        """
        return self._update(items, mode=mode)

    def delete(self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False) -> None:
        """Delete relationships from CDF.

        Args:
            items: List of ExternalId objects to delete.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        """
        self._request_no_response(items, "delete", extra_body={"ignoreUnknownIds": ignore_unknown_ids})

    def paginate(
        self,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[RelationshipResponse]:
        """Get a page of relationships from CDF.

        Args:
            limit: Maximum number of relationships to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of RelationshipResponse objects.
        """
        return self._paginate(
            cursor=cursor,
            limit=limit,
        )

    def iterate(
        self,
        limit: int | None = None,
    ) -> Iterable[list[RelationshipResponse]]:
        """Iterate over all relationships in CDF.

        Args:
            limit: Maximum total number of relationships to return.

        Returns:
            Iterable of lists of RelationshipResponse objects.
        """
        return self._iterate(limit=limit)

    def list(self, limit: int | None = None) -> list[RelationshipResponse]:
        """List all relationships in CDF.

        Args:
            limit: Maximum total number of relationships to return.

        Returns:
            List of RelationshipResponse objects.
        """
        return self._list(limit=limit)
