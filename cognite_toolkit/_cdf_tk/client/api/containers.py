"""Containers API for managing CDF data modeling containers.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Containers/operation/ApplyContainers
"""

from collections.abc import Iterable, Sequence

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, Endpoint, PagedResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.request_classes.filters import ContainerFilter
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    ContainerReference,
    ContainerRequest,
    ContainerResponse,
)


class ContainersAPI(CDFResourceAPI[ContainerReference, ContainerRequest, ContainerResponse]):
    """API for managing CDF data modeling containers.

    Containers use an apply/upsert pattern for create and update operations.
    """

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "upsert": Endpoint(method="POST", path="/models/containers", item_limit=100),
                "retrieve": Endpoint(method="POST", path="/models/containers/byids", item_limit=100),
                "delete": Endpoint(method="POST", path="/models/containers/delete", item_limit=100),
                "list": Endpoint(method="GET", path="/models/containers", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[ContainerResponse]:
        return PagedResponse[ContainerResponse].model_validate_json(response.body)

    def create(self, items: Sequence[ContainerRequest]) -> list[ContainerResponse]:
        """Create (create or update) containers in CDF.

        Args:
            items: List of ContainerRequest objects to apply.

        Returns:
            List of applied ContainerResponse objects.
        """
        return self._request_item_response(items, "upsert")

    def update(self, items: Sequence[ContainerRequest]) -> list[ContainerResponse]:
        """Update (create or update) containers in CDF.

        Args:
            items: List of ContainerRequest objects to apply.
        Returns:
            List of applied ContainerResponse objects.
        """
        return self._request_item_response(items, "upsert")

    def retrieve(self, items: Sequence[ContainerReference]) -> list[ContainerResponse]:
        """Retrieve containers from CDF.

        Args:
            items: List of ContainerReference objects to retrieve.

        Returns:
            List of retrieved ContainerResponse objects.
        """
        return self._request_item_response(items, method="retrieve")

    def delete(self, items: Sequence[ContainerReference]) -> None:
        """Delete containers from CDF.

        Args:
            items: List of ContainerReference objects to delete.
        """
        self._request_no_response(items, "delete")

    def paginate(
        self,
        filter: ContainerFilter | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[ContainerResponse]:
        """Get a page of containers from CDF.

        Args:
            filter: ContainerFilter to filter containers.
            limit: Maximum number of containers to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of ContainerResponse objects.
        """
        return self._paginate(
            cursor=cursor,
            limit=limit,
            params=filter.dump() if filter else None,
        )

    def iterate(
        self,
        filter: ContainerFilter | None = None,
        limit: int | None = None,
    ) -> Iterable[list[ContainerResponse]]:
        """Iterate over all containers in CDF.

        Args:
            filter: ContainerFilter to filter containers.
            limit: Maximum total number of containers to return.

        Returns:
            Iterable of lists of ContainerResponse objects.
        """
        return self._iterate(
            limit=limit,
            params=filter.dump() if filter else None,
        )

    def list(self, filter: ContainerFilter | None = None, limit: int | None = None) -> list[ContainerResponse]:
        """List all containers in CDF.

        Args:
            filter: ContainerFilter to filter containers.
            limit: Maximum total number of containers to return.

        Returns:
            List of ContainerResponse objects.
        """
        return self._list(limit=limit, params=filter.dump() if filter else None)
