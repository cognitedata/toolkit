from collections.abc import Iterable, Sequence
from typing import Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.robotics._map import RobotMapRequest, RobotMapResponse


class MapsAPI(CDFResourceAPI[ExternalId, RobotMapRequest, RobotMapResponse]):
    """API for managing Map resources in CDF."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/robotics/maps", item_limit=1000, concurrency_max_workers=1),
                "retrieve": Endpoint(
                    method="POST", path="/robotics/maps/byids", item_limit=1000, concurrency_max_workers=1
                ),
                "update": Endpoint(
                    method="POST", path="/robotics/maps/update", item_limit=1000, concurrency_max_workers=1
                ),
                "delete": Endpoint(
                    method="POST", path="/robotics/maps/delete", item_limit=1000, concurrency_max_workers=1
                ),
                "list": Endpoint(method="GET", path="/robotics/maps", item_limit=1000),
            },
            disable_gzip=True,
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[RobotMapResponse]:
        return PagedResponse[RobotMapResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse) -> ResponseItems[ExternalId]:
        return ResponseItems[ExternalId].model_validate_json(response.body)

    def create(self, items: Sequence[RobotMapRequest]) -> list[RobotMapResponse]:
        """Create maps in CDF.

        Args:
            items: List of RobotMapRequest objects to create.
        Returns:
            List of created RobotMapResponse objects.
        """
        return self._request_item_response(items, "create")

    def retrieve(self, items: Sequence[ExternalId]) -> list[RobotMapResponse]:
        """Retrieve maps from CDF.

        Args:
            items: List of ExternalId objects to retrieve.
        Returns:
            List of retrieved RobotMapResponse objects.
        """
        return self._request_item_response(items, method="retrieve")

    def update(
        self, items: Sequence[RobotMapRequest], mode: Literal["patch", "replace"] = "replace"
    ) -> list[RobotMapResponse]:
        """Update maps in CDF.

        Args:
            items: List of RobotMapRequest objects to update.
            mode: Update mode, either "patch" or "replace".

        Returns:
            List of updated RobotMapResponse objects.
        """
        return self._update(items, mode=mode)

    def delete(self, items: Sequence[ExternalId]) -> None:
        """Delete maps from CDF.

        Args:
            items: List of ExternalId objects to delete.
        """
        self._request_no_response(items, "delete")

    def paginate(
        self,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[RobotMapResponse]:
        """Paginate over maps in CDF.

        Args:
            limit: Maximum number of items per page.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of RobotMapResponse objects.
        """
        return self._paginate(cursor=cursor, limit=limit)

    def iterate(
        self,
        limit: int | None = 100,
    ) -> Iterable[list[RobotMapResponse]]:
        """Iterate over all maps in CDF.

        Args:
            limit: Maximum number of items to return. None returns all.

        Returns:
            Iterable of lists of RobotMapResponse objects.
        """
        return self._iterate(limit=limit)

    def list(self, limit: int | None = 100) -> list[RobotMapResponse]:
        """List all maps in CDF.

        Args:
            limit: Maximum number of items to return. None returns all.

        Returns:
            List of RobotMapResponse objects.
        """
        return self._list(limit=limit)
