from collections.abc import Iterable, Sequence
from typing import Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse2, SuccessResponse2
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.robotics._data_post_processing import (
    RobotDataPostProcessingRequest,
    RobotDataPostProcessingResponse,
)


class DataPostProcessingResourceAPI(
    CDFResourceAPI[ExternalId, RobotDataPostProcessingRequest, RobotDataPostProcessingResponse]
):
    """API for managing DataPostProcessing resources in CDF."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(
                    method="POST", path="/robotics/data_postprocessing", item_limit=1000, concurrency_max_workers=1
                ),
                "retrieve": Endpoint(
                    method="POST",
                    path="/robotics/data_postprocessing/byids",
                    item_limit=1000,
                    concurrency_max_workers=1,
                ),
                "update": Endpoint(
                    method="POST",
                    path="/robotics/data_postprocessing/update",
                    item_limit=1000,
                    concurrency_max_workers=1,
                ),
                "delete": Endpoint(
                    method="POST",
                    path="/robotics/data_postprocessing/delete",
                    item_limit=1000,
                    concurrency_max_workers=1,
                ),
                "list": Endpoint(method="GET", path="/robotics/data_postprocessing", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse2 | ItemsSuccessResponse2
    ) -> PagedResponse[RobotDataPostProcessingResponse]:
        return PagedResponse[RobotDataPostProcessingResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse2) -> ResponseItems[ExternalId]:
        return ResponseItems[ExternalId].model_validate_json(response.body)

    def create(self, items: Sequence[RobotDataPostProcessingRequest]) -> list[RobotDataPostProcessingResponse]:
        """Create data post-processing configurations in CDF.

        Args:
            items: List of RobotDataPostProcessingRequest objects to create.
        Returns:
            List of created RobotDataPostProcessingResponse objects.
        """
        return self._request_item_response(items, "create")

    def retrieve(
        self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False
    ) -> list[RobotDataPostProcessingResponse]:
        """Retrieve data post-processing configurations from CDF.

        Args:
            items: List of ExternalId objects to retrieve.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        Returns:
            List of retrieved RobotDataPostProcessingResponse objects.
        """
        return self._request_item_response(
            items, method="retrieve", extra_body={"ignoreUnknownIds": ignore_unknown_ids}
        )

    def update(
        self, items: Sequence[RobotDataPostProcessingRequest], mode: Literal["patch", "replace"] = "replace"
    ) -> list[RobotDataPostProcessingResponse]:
        """Update data post-processing configurations in CDF.

        Args:
            items: List of RobotDataPostProcessingRequest objects to update.
            mode: Update mode, either "patch" or "replace".

        Returns:
            List of updated RobotDataPostProcessingResponse objects.
        """
        return self._update(items, mode=mode)

    def delete(self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False) -> None:
        """Delete data post-processing configurations from CDF.

        Args:
            items: List of ExternalId objects to delete.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        """
        self._request_no_response(items, "delete", extra_body={"ignoreUnknownIds": ignore_unknown_ids})

    def paginate(
        self,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[RobotDataPostProcessingResponse]:
        """Paginate over data post-processing configurations in CDF.

        Args:
            limit: Maximum number of items per page.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of RobotDataPostProcessingResponse objects.
        """
        return self._paginate(cursor=cursor, limit=limit)

    def iterate(
        self,
        limit: int | None = 100,
    ) -> Iterable[list[RobotDataPostProcessingResponse]]:
        """Iterate over all data post-processing configurations in CDF.

        Args:
            limit: Maximum number of items to return. None returns all.

        Returns:
            Iterable of lists of RobotDataPostProcessingResponse objects.
        """
        return self._iterate(limit=limit)

    def list(self, limit: int | None = 100) -> list[RobotDataPostProcessingResponse]:
        """List all data post-processing configurations in CDF.

        Args:
            limit: Maximum number of items to return. None returns all.

        Returns:
            List of RobotDataPostProcessingResponse objects.
        """
        return self._list(limit=limit)
