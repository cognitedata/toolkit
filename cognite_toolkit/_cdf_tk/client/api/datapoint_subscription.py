from collections.abc import Iterable, Sequence
from typing import Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.datapoint_subscription import (
    DatapointSubscriptionRequest,
    DatapointSubscriptionResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExternalId


class DatapointSubscriptionAPI(CDFResourceAPI[ExternalId, DatapointSubscriptionRequest, DatapointSubscriptionResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(
                    method="POST", path="/timeseries/subscriptions", item_limit=1000, concurrency_max_workers=1
                ),
                "retrieve": Endpoint(
                    method="POST",
                    path="/timeseries/subscriptions/byids",
                    item_limit=1000,
                    concurrency_max_workers=1,
                ),
                "update": Endpoint(
                    method="POST",
                    path="/timeseries/subscriptions/update",
                    item_limit=1000,
                    concurrency_max_workers=1,
                ),
                "delete": Endpoint(
                    method="POST",
                    path="/timeseries/subscriptions/delete",
                    item_limit=1000,
                    concurrency_max_workers=1,
                ),
                "list": Endpoint(method="GET", path="/timeseries/subscriptions", item_limit=100),
            },
        )

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

    def update(
        self, items: Sequence[DatapointSubscriptionRequest], mode: Literal["patch", "replace"] = "replace"
    ) -> list[DatapointSubscriptionResponse]:
        """Update datapoint subscriptions in CDF.

        Args:
            items: List of DatapointSubscriptionRequest objects to update.
            mode: Update mode, either "patch" or "replace".

        Returns:
            List of updated DatapointSubscriptionResponse objects.
        """
        return self._update(items, mode=mode)

    def delete(self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False) -> None:
        """Delete datapoint subscriptions from CDF.

        Args:
            items: List of ExternalId objects to delete.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        """
        self._request_no_response(items, "delete", extra_body={"ignoreUnknownIds": ignore_unknown_ids})

    def paginate(
        self,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[DatapointSubscriptionResponse]:
        """Paginate over datapoint subscriptions in CDF.

        Args:
            limit: Maximum number of items to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of DatapointSubscriptionResponse objects.
        """
        return self._paginate(cursor=cursor, limit=limit)

    def iterate(
        self,
        limit: int | None = 100,
    ) -> Iterable[list[DatapointSubscriptionResponse]]:
        """Iterate over all datapoint subscriptions in CDF.

        Args:
            limit: Maximum number of items to return per page.

        Returns:
            Iterable of lists of DatapointSubscriptionResponse objects.
        """
        return self._iterate(limit=limit)

    def list(
        self,
        limit: int | None = 100,
    ) -> list[DatapointSubscriptionResponse]:
        """List all datapoint subscriptions in CDF.

        Returns:
            List of DatapointSubscriptionResponse objects.
        """
        return self._list(limit=limit)
