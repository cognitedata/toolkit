from collections.abc import Iterable, Sequence
from typing import Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, InternalId
from cognite_toolkit._cdf_tk.client.resource_classes.chart_scheduled_calculation import (
    ChartScheduledCalculationRequest,
    ChartScheduledCalculationResponse,
)


class ChartScheduledCalculationsAPI(CDFResourceAPI[ChartScheduledCalculationResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/calculations/schedules", item_limit=1000),
                "retrieve": Endpoint(method="POST", path="/calculations/schedules/byids", item_limit=1000),
                "update": Endpoint(method="POST", path="/calculations/schedules/update", item_limit=1000),
                "delete": Endpoint(method="POST", path="/calculations/schedules/delete", item_limit=1000),
                "list": Endpoint(method="GET", path="/calculations/schedules", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[ChartScheduledCalculationResponse]:
        return PagedResponse[ChartScheduledCalculationResponse].model_validate_json(response.body)

    def create(self, items: Sequence[ChartScheduledCalculationRequest]) -> list[ChartScheduledCalculationResponse]:
        """Create chart scheduled calculations in CDF.

        Args:
            items: Scheduled calculation request objects to create.
        Returns:
            Created scheduled calculation response objects.
        """
        return self._request_item_response(items, "create")

    def retrieve(self, items: Sequence[InternalId | ExternalId]) -> list[ChartScheduledCalculationResponse]:
        """Retrieve chart scheduled calculations by internal or external ID.

        Args:
            items: Identifiers to retrieve.
        Returns:
            Retrieved scheduled calculation response objects.
        """
        return self._request_item_response(items, "retrieve")

    def delete(self, items: Sequence[InternalId | ExternalId]) -> None:
        """Delete chart scheduled calculations by internal or external ID.

        Args:
            items: Identifiers to delete.
        """
        self._request_no_response(items, "delete")

    def update(
        self, items: Sequence[ChartScheduledCalculationRequest], mode: Literal["patch", "replace"] = "replace"
    ) -> list[ChartScheduledCalculationResponse]:
        """Update chart scheduled calculations.

        Args:
            items: Scheduled calculation request objects to update.
            mode: Update mode, either "patch" or "replace".

        Returns:
            Updated scheduled calculation response objects.
        """
        return self._update(items, mode=mode)

    def paginate(
        self,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[ChartScheduledCalculationResponse]:
        """Fetch a page of chart scheduled calculations from CDF.

        Args:
            limit: Maximum number of items to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of scheduled calculation response objects.
        """
        return self._paginate(
            cursor=cursor,
            limit=limit,
        )

    def iterate(
        self,
        limit: int | None = 100,
    ) -> Iterable[list[ChartScheduledCalculationResponse]]:
        """Iterate over chart scheduled calculations in CDF.

        Args:
            limit: Maximum number of items to return per page.

        Returns:
            Iterable of lists of scheduled calculation response objects.
        """
        return self._iterate(limit=limit)

    def list(
        self,
        limit: int | None = 100,
    ) -> list[ChartScheduledCalculationResponse]:
        """List chart scheduled calculations in CDF.

        Args:
            limit: Maximum total number of items to return across all pages.

        Returns:
            Scheduled calculation response objects.
        """
        return self._list(limit=limit)
