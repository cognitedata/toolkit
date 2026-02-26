from collections.abc import Iterable, Sequence
from typing import Any

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers._references import DatapointSubscriptionTimeSeriesId
from cognite_toolkit._cdf_tk.client.identifiers.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.datapoint_subscription import (
    DatapointSubscriptionRequest,
    DatapointSubscriptionResponse,
    DatapointSubscriptionUpdateRequest,
)


class DatapointSubscriptionsAPI(CDFResourceAPI[DatapointSubscriptionResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/timeseries/subscriptions", item_limit=1),
                "retrieve": Endpoint(method="POST", path="/timeseries/subscriptions/byids", item_limit=1),
                "update": Endpoint(method="POST", path="/timeseries/subscriptions/update", item_limit=1),
                "delete": Endpoint(method="POST", path="/timeseries/subscriptions/delete", item_limit=1),
                "list": Endpoint(method="GET", path="/timeseries/subscriptions", item_limit=100),
            },
        )
        self._list_members = Endpoint(method="GET", path="/timeseries/subscriptions/members", item_limit=100)

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[DatapointSubscriptionResponse]:
        return PagedResponse[DatapointSubscriptionResponse].model_validate_json(response.body)

    def create(self, items: Sequence[DatapointSubscriptionRequest]) -> list[DatapointSubscriptionResponse]:
        """Create datapoint subscriptions in CDF.

        Args:
            items: List of DatapointSubscriptionRequest objects to create.
        Returns:
            List of created DatapointSubscriptionResponse objects.
        """
        return self._request_item_response(items, "create")

    def retrieve(
        self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False
    ) -> list[DatapointSubscriptionResponse]:
        """Retrieve datapoint subscriptions from CDF.

        Args:
            items: List of ExternalId objects to retrieve.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        Returns:
            List of retrieved DatapointSubscriptionResponse objects.
        """
        return self._request_item_response(
            items, method="retrieve", extra_body={"ignoreUnknownIds": ignore_unknown_ids}
        )

    def update(self, items: Sequence[DatapointSubscriptionUpdateRequest]) -> list[DatapointSubscriptionResponse]:
        """Update datapoint subscriptions in CDF.

        Args:
            items: List of DatapointSubscriptionUpdateRequest objects to update.

        Returns:
            List of updated DatapointSubscriptionResponse objects.
        """
        return self._request_item_response(items, "update")

    def delete(self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False) -> None:
        """Delete datapoint subscriptions from CDF.

        Args:
            items: List of ExternalId objects to delete.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        """
        self._request_no_response(items, "delete", extra_body={"ignoreUnknownIds": ignore_unknown_ids})

    def list_members(self, external_id: str, limit: int | None = 100) -> list[DatapointSubscriptionTimeSeriesId]:
        """List time series IDs subscribed to by a datapoint subscription.

        Args:
            external_id: External ID of the datapoint subscription.
            limit: Maximum number of items to return.

        Returns:
            List of time series IDs subscribed to by the datapoint subscription.
        """
        cursor: str | None = None
        total: int = 0
        endpoint = self._list_members
        result: list[DatapointSubscriptionTimeSeriesId] = []
        while cursor is not None or total == 0:
            page_limit = endpoint.item_limit if limit is None else min(limit - total, endpoint.item_limit)
            parameters: dict[str, Any] = {"externalId": external_id, "limit": page_limit}
            if cursor is not None:
                parameters["cursor"] = cursor
            response = self._http_client.request_single_retries(
                RequestMessage(
                    endpoint_url=self._http_client.config.create_api_url(endpoint.path),
                    method=endpoint.method,
                    parameters=parameters,
                )
            ).get_success_or_raise()
            page_response = PagedResponse[DatapointSubscriptionTimeSeriesId].model_validate_json(response.body)
            result.extend(page_response.items)
            total += len(page_response.items)
            if (limit is not None and total >= limit) or not page_response.items:
                break
            cursor = page_response.next_cursor

        return result

    def paginate(self, limit: int = 100, cursor: str | None = None) -> PagedResponse[DatapointSubscriptionResponse]:
        """Paginate over datapoint subscriptions in CDF.

        Args:
            limit: Maximum number of items to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of DatapointSubscriptionResponse objects.
        """
        return self._paginate(cursor=cursor, limit=limit)

    def iterate(self, limit: int | None = 100) -> Iterable[list[DatapointSubscriptionResponse]]:
        """Iterate over all datapoint subscriptions in CDF.

        Args:
            limit: Maximum number of items to return per page.

        Returns:
            Iterable of lists of DatapointSubscriptionResponse objects.
        """
        return self._iterate(limit=limit)

    def list(self, limit: int | None = 100) -> list[DatapointSubscriptionResponse]:
        """List all datapoint subscriptions in CDF.

        Returns:
            List of DatapointSubscriptionResponse objects.
        """
        return self._list(limit=limit)
