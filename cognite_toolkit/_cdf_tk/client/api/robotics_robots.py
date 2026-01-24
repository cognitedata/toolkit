from collections.abc import Iterable, Sequence
from typing import Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import DataSetId
from cognite_toolkit._cdf_tk.client.resource_classes.robotics._robot import RobotRequest, RobotResponse


class RobotsAPI(CDFResourceAPI[DataSetId, RobotRequest, RobotResponse]):
    """API for managing Robot resources in CDF."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/robotics/robots", item_limit=1000, concurrency_max_workers=1),
                "retrieve": Endpoint(
                    method="POST", path="/robotics/robots/byids", item_limit=1000, concurrency_max_workers=1
                ),
                "update": Endpoint(
                    method="POST", path="/robotics/robots/update", item_limit=1000, concurrency_max_workers=1
                ),
                "delete": Endpoint(
                    method="POST", path="/robotics/robots/delete", item_limit=1000, concurrency_max_workers=1
                ),
                "list": Endpoint(method="GET", path="/robotics/robots", item_limit=1000),
            },
            disable_gzip=True,
        )

    def _validate_page_response(self, response: SuccessResponse | ItemsSuccessResponse) -> PagedResponse[RobotResponse]:
        return PagedResponse[RobotResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse) -> ResponseItems[DataSetId]:
        return ResponseItems[DataSetId].model_validate_json(response.body)

    def create(self, items: Sequence[RobotRequest]) -> list[RobotResponse]:
        """Create robots in CDF.

        Args:
            items: List of RobotRequest objects to create.
        Returns:
            List of created RobotResponse objects.
        """
        return self._request_item_response(items, "create")

    def retrieve(self, items: Sequence[DataSetId]) -> list[RobotResponse]:
        """Retrieve robots from CDF.

        Args:
            items: List of DataSetId objects to retrieve.
        Returns:
            List of retrieved RobotResponse objects.
        """
        return self._request_item_response(items, method="retrieve")

    def update(
        self, items: Sequence[RobotRequest], mode: Literal["patch", "replace"] = "replace"
    ) -> list[RobotResponse]:
        """Update robots in CDF.

        Args:
            items: List of RobotRequest objects to update.
            mode: Update mode, either "patch" or "replace".

        Returns:
            List of updated RobotResponse objects.
        """
        return self._update(items, mode=mode)

    def delete(self, items: Sequence[DataSetId]) -> None:
        """Delete robots from CDF.

        Args:
            items: List of DataSetId objects to delete.
        """
        self._request_no_response(items, "delete")

    def paginate(
        self,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[RobotResponse]:
        """Paginate over robots in CDF.

        Args:
            limit: Maximum number of items per page.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of RobotResponse objects.
        """
        return self._paginate(cursor=cursor, limit=limit)

    def iterate(
        self,
        limit: int | None = 100,
    ) -> Iterable[list[RobotResponse]]:
        """Iterate over all robots in CDF.

        Args:
            limit: Maximum number of items to return. None returns all.

        Returns:
            Iterable of lists of RobotResponse objects.
        """
        return self._iterate(limit=limit)

    def list(self, limit: int | None = 100) -> list[RobotResponse]:
        """List all robots in CDF.

        Args:
            limit: Maximum number of items to return. None returns all.

        Returns:
            List of RobotResponse objects.
        """
        return self._list(limit=limit)
