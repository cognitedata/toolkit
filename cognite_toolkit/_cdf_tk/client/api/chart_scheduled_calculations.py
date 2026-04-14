from collections.abc import Sequence
from typing import Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.chart_scheduled_calculation import (
    ChartScheduledCalculationListResponse,
    ChartScheduledCalculationRequest,
    ChartScheduledCalculationResponse,
)


class ChartScheduledCalculationsAPI(CDFResourceAPI[ChartScheduledCalculationResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/calculations/schedules", item_limit=1),
                "retrieve": Endpoint(method="POST", path="/calculations/schedules/byids", item_limit=1000),
                "update": Endpoint(method="POST", path="/calculations/schedules/update", item_limit=1),
                "delete": Endpoint(method="POST", path="/calculations/schedules/delete", item_limit=1),
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

    def retrieve(
        self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False
    ) -> list[ChartScheduledCalculationResponse]:
        """Retrieve chart scheduled calculations by internal or external ID.

        Args:
            items: Identifiers to retrieve.
            ignore_unknown_ids: Whether to ignore unknown internal or external IDs.
        Returns:
            Retrieved scheduled calculation response objects.
        """
        if ignore_unknown_ids:
            return self._request_item_split_retries(items, "retrieve")
        else:
            return self._request_item_response(items, "retrieve")

    def delete(self, items: Sequence[ExternalId]) -> None:
        """Delete chart scheduled calculations by internal or external ID.

        Args:
            items: Identifiers to delete.
        """
        self._request_no_response(items, "delete")

    def update(
        self, items: Sequence[ChartScheduledCalculationRequest], mode: Literal["replace"] = "replace"
    ) -> list[ChartScheduledCalculationResponse]:
        """Update chart scheduled calculations.

        Args:
            items: Scheduled calculation request objects to update.
            mode: Update mode, currently only "replace" is supported.

        Returns:
            Updated scheduled calculation response objects.
        """
        return self._update(items, mode=mode)

    def list(self) -> list[ChartScheduledCalculationListResponse]:
        """List chart scheduled calculations in CDF.

        Returns:
            Scheduled calculation response objects.
        """
        endpoint = self._method_endpoint_map["list"]
        request = RequestMessage(
            endpoint_url=self._http_client.config.create_api_url(endpoint.path),
            method=endpoint.method,
        )
        response = self._http_client.request_single_retries(request).get_success_or_raise(request)
        return PagedResponse[ChartScheduledCalculationListResponse].model_validate_json(response.body).items
