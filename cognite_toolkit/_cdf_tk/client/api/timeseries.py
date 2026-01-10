from collections.abc import Sequence
from typing import Any, Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.data_classes.identifiers import InternalOrExternalId
from cognite_toolkit._cdf_tk.client.data_classes.timeseries import TimeSeriesRequest, TimeSeriesResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse2, SuccessResponse2


class TimeSeriesAPI(CDFResourceAPI[InternalOrExternalId, TimeSeriesRequest, TimeSeriesResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/timeseries", item_limit=1000, concurrency_max_workers=1),
                "retrieve": Endpoint(
                    method="POST", path="/timeseries/byids", item_limit=1000, concurrency_max_workers=1
                ),
                "update": Endpoint(
                    method="POST", path="/timeseries/update", item_limit=1000, concurrency_max_workers=1
                ),
                "delete": Endpoint(
                    method="POST", path="/timeseries/delete", item_limit=1000, concurrency_max_workers=1
                ),
                "list": Endpoint(method="POST", path="/timeseries/list", item_limit=1000),
            },
        )

    def _page_response(self, response: SuccessResponse2 | ItemsSuccessResponse2) -> PagedResponse[TimeSeriesResponse]:
        return PagedResponse[TimeSeriesResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse2) -> ResponseItems[InternalOrExternalId]:
        return ResponseItems[InternalOrExternalId].model_validate_json(response.body)

    def create(self, items: Sequence[TimeSeriesRequest]) -> list[TimeSeriesResponse]:
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
        return self._request_item_response(
            items, method="retrieve", extra_body={"ignoreUnknownIds": ignore_unknown_ids}
        )

    def update(
        self, items: Sequence[TimeSeriesRequest], mode: Literal["patch", "replace"] = "replace"
    ) -> list[TimeSeriesResponse]:
        """Update time series in CDF.

        Args:
            items: List of TimeSeriesRequest objects to update.
            mode: Update mode, either "patch" or "replace".

        Returns:
            List of updated TimeSeriesResponse objects.
        """
        return self._update(items, mode=mode)

    def delete(self, items: Sequence[InternalOrExternalId], ignore_unknown_ids: bool = False) -> None:
        """Delete time series from CDF.

        Args:
            items: List of InternalOrExternalId objects to delete.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        """
        self._request_no_response(items, "delete", extra_body={"ignoreUnknownIds": ignore_unknown_ids})

    def iterate(
        self,
        data_set_external_ids: list[str] | None = None,
        asset_subtree_external_ids: list[str] | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[TimeSeriesResponse]:
        """Iterate over all time series in CDF.

        Args:
            data_set_external_ids: Filter by data set external IDs.
            asset_subtree_external_ids: Filter by asset subtree external IDs.
            limit: Maximum number of items to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of TimeSeriesResponse objects.
        """
        filter_: dict[str, Any] = {}
        if asset_subtree_external_ids:
            filter_["assetSubtreeIds"] = [{"externalId": ext_id} for ext_id in asset_subtree_external_ids]
        if data_set_external_ids:
            filter_["dataSetIds"] = [{"externalId": ds_id} for ds_id in data_set_external_ids]

        return self._iterate(
            cursor=cursor,
            limit=limit,
            body={"filter": filter_ or None},
        )

    def list(
        self,
        limit: int | None = 100,
    ) -> list[TimeSeriesResponse]:
        """List all time series in CDF.

        Returns:
            List of TimeSeriesResponse objects.
        """
        return self._list(limit=limit)
