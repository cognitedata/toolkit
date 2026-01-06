from collections.abc import Sequence

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.data_classes.identifiers import InternalOrExternalId
from cognite_toolkit._cdf_tk.client.data_classes.timeseries import TimeSeriesRequest, TimeSeriesResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, SuccessResponse2


class TimeSeriesAPI(CDFResourceAPI[InternalOrExternalId, TimeSeriesRequest, TimeSeriesResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/timeseries", item_limit=1000, concurrency_max_workers=1),
                "retrieve": Endpoint(
                    method="POST", path="/timeseries/byids", item_limit=1000, concurrency_max_workers=1
                ),
                "delete": Endpoint(
                    method="POST", path="/timeseries/delete", item_limit=1000, concurrency_max_workers=1
                ),
                "list": Endpoint(method="GET", path="/timeseries", item_limit=1000),
            },
        )

    def _page_response(self, response: SuccessResponse2) -> PagedResponse[TimeSeriesResponse]:
        return PagedResponse[TimeSeriesResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse2) -> ResponseItems[InternalOrExternalId]:
        return ResponseItems[InternalOrExternalId].model_validate_json(response.body)

    def create(self, items: list[TimeSeriesRequest]) -> list[TimeSeriesResponse]:
        """Create time series in CDF.

        Args:
            items: List of TimeSeriesRequest objects to create.
        Returns:
            List of created TimeSeriesResponse objects.
        """
        return self._request_item_response(items, "create")

    def retrieve(
        self, items: Sequence[InternalOrExternalId], ignore_unknown_ids: bool = False
    ) -> list[TimeSeriesResponse]:
        """Retrieve time series from CDF.

        Args:
            items: List of InternalOrExternalId objects to retrieve.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        Returns:
            List of retrieved TimeSeriesResponse objects.
        """
        return self._request_item_response(items, method="retrieve", params={"ignoreUnknownIds": ignore_unknown_ids})

    def delete(self, items: list[InternalOrExternalId], ignore_unknown_ids: bool = False) -> None:
        """Delete time series from CDF.

        Args:
            items: List of InternalOrExternalId objects to delete.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        """
        self._request_no_response(items, "delete", params={"ignoreUnknownIds": ignore_unknown_ids})

    def iterate(
        self,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[TimeSeriesResponse]:
        """Iterate over all time series in CDF.

        Returns:
            PagedResponse of TimeSeriesResponse objects.
        """
        return self._iterate(cursor=cursor, limit=limit)

    def list(
        self,
        limit: int | None = 100,
    ) -> list[TimeSeriesResponse]:
        """List all time series in CDF.

        Returns:
            List of TimeSeriesResponse objects.
        """
        return self._list(limit=limit)
