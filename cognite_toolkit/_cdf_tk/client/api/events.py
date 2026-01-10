from collections.abc import Sequence
from typing import Any, Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.data_classes.event import EventRequest, EventResponse
from cognite_toolkit._cdf_tk.client.data_classes.identifiers import InternalOrExternalId
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse2, SuccessResponse2


class EventsAPI(CDFResourceAPI[InternalOrExternalId, EventRequest, EventResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/events", item_limit=1000, concurrency_max_workers=1),
                "retrieve": Endpoint(method="POST", path="/events/byids", item_limit=1000, concurrency_max_workers=1),
                "update": Endpoint(method="POST", path="/events/update", item_limit=1000, concurrency_max_workers=1),
                "delete": Endpoint(method="POST", path="/events/delete", item_limit=1000, concurrency_max_workers=1),
                "list": Endpoint(method="POST", path="/events/list", item_limit=1000),
            },
        )

    def _page_response(self, response: SuccessResponse2 | ItemsSuccessResponse2) -> PagedResponse[EventResponse]:
        return PagedResponse[EventResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse2) -> ResponseItems[InternalOrExternalId]:
        return ResponseItems[InternalOrExternalId].model_validate_json(response.body)

    def create(self, items: Sequence[EventRequest]) -> list[EventResponse]:
        """Create events in CDF.

        Args:
            items: List of EventRequest objects to create.
        Returns:
            List of created EventResponse objects.
        """
        return self._request_item_response(items, "create")

    def retrieve(self, items: Sequence[InternalOrExternalId], ignore_unknown_ids: bool = False) -> list[EventResponse]:
        """Retrieve events from CDF.

        Args:
            items: List of InternalOrExternalId objects to retrieve.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        Returns:
            List of retrieved EventResponse objects.
        """
        return self._request_item_response(
            items, method="retrieve", extra_body={"ignoreUnknownIds": ignore_unknown_ids}
        )

    def update(
        self, items: Sequence[EventRequest], mode: Literal["patch", "replace"] = "replace"
    ) -> list[EventResponse]:
        """Update events in CDF.

        Args:
            items: List of EventRequest objects to update.
            mode: Update mode, either "patch" or "replace".

        Returns:
            List of updated EventResponse objects.
        """
        return self._update(items, mode=mode)

    def delete(self, items: Sequence[InternalOrExternalId], ignore_unknown_ids: bool = False) -> None:
        """Delete events from CDF.

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
    ) -> PagedResponse[EventResponse]:
        """Iterate over all events in CDF.

        Args:
            data_set_external_ids: Filter by data set external IDs.
            asset_subtree_external_ids: Filter by asset subtree external IDs.
            limit: Maximum number of items to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of EventResponse objects.
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
    ) -> list[EventResponse]:
        """List all events in CDF.

        Returns:
            List of EventResponse objects.
        """
        return self._list(limit=limit)
