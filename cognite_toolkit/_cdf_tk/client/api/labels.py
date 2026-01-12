from collections.abc import Sequence
from typing import Any

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.data_classes.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.data_classes.label import LabelRequest, LabelResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, SuccessResponse2


class LabelsAPI(CDFResourceAPI[ExternalId, LabelRequest, LabelResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/labels", item_limit=1000, concurrency_max_workers=1),
                "delete": Endpoint(method="POST", path="/labels/delete", item_limit=1000, concurrency_max_workers=1),
                "list": Endpoint(method="GET", path="/labels", item_limit=1000),
            },
        )

    def _page_response(self, response: SuccessResponse2) -> PagedResponse[LabelResponse]:
        return PagedResponse[LabelResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse2) -> ResponseItems[ExternalId]:
        return ResponseItems[ExternalId].model_validate_json(response.body)

    def create(self, items: Sequence[LabelRequest]) -> list[LabelResponse]:
        """Create labels in CDF.

        Args:
            items: List of LabelRequest objects to create.
        Returns:
            List of created LabelResponse objects.
        """
        return self._request_item_response(items, "create")

    def delete(self, items: Sequence[ExternalId]) -> None:
        """Delete labels from CDF.

        Args:
            items: List of ExternalId objects to delete.
        """
        self._request_no_response(items, "delete")

    def iterate(
        self,
        name: str | None = None,
        data_set_external_ids: list[str] | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[LabelResponse]:
        """Iterate over all labels in CDF.

        Args:
            name: Filter by label name (prefix match).
            data_set_external_ids: Filter by data set external IDs.
            limit: Maximum number of items to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of LabelResponse objects.
        """
        params: dict[str, Any] = {}
        if name:
            params["name"] = name
        if data_set_external_ids:
            params["dataSetIds"] = [{"externalId": ds_id} for ds_id in data_set_external_ids]

        return self._iterate(
            cursor=cursor,
            limit=limit,
            params=params,
        )

    def list(
        self,
        limit: int | None = 100,
    ) -> list[LabelResponse]:
        """List all labels in CDF.

        Returns:
            List of LabelResponse objects.
        """
        return self._list(limit=limit)
