from collections.abc import Sequence
from typing import Any

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.legacy.charts import (
    ChartRequest,
    ChartResponse,
    Visibility,
)


class ChartsAPI(CDFResourceAPI[ExternalId, ChartRequest, ChartResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(
                    method="PUT", path="/storage/charts/charts", item_limit=1000, concurrency_max_workers=1
                ),
                "retrieve": Endpoint(
                    method="POST", path="/storage/charts/charts/byids", item_limit=1000, concurrency_max_workers=1
                ),
                "delete": Endpoint(
                    method="POST", path="/storage/charts/charts/delete", item_limit=1000, concurrency_max_workers=1
                ),
                "list": Endpoint(method="POST", path="/storage/charts/charts/list", item_limit=1000),
            },
        )

    def _make_url(self, path: str = "") -> str:
        return self._http_client.config.create_app_url(path)

    def _validate_page_response(self, response: SuccessResponse | ItemsSuccessResponse) -> PagedResponse[ChartResponse]:
        return PagedResponse[ChartResponse].model_validate_json(response.body)

    def create(self, items: Sequence[ChartRequest]) -> list[ChartResponse]:
        """Create charts in CDF.

        Args:
            items: List of chart Request objects to create.
        Returns:
            List of created chart Response objects.
        """
        return self._request_item_response(items, "create")

    def retrieve(self, items: Sequence[ExternalId]) -> list[ChartResponse]:
        """Retrieve charts from CDF by external ID.

        Args:
            items: List of ExternalId objects to retrieve.
        Returns:
            List of retrieved chart Response objects.
        """
        return self._request_item_response(items, "retrieve")

    def delete(self, items: Sequence[ExternalId]) -> None:
        """Delete charts from CDF by external ID.

        Args:
            items: List of ExternalId objects to delete.
        """
        self._request_no_response(items, "delete")

    def list(
        self, visibility: Visibility | None = None, is_owned: bool | None = None, limit: int = 100
    ) -> list[ChartResponse]:
        """List charts based on visibility and ownership.

        Args:
            visibility: Visibility of the charts to list, either 'PUBLIC' or 'PRIVATE'.
            is_owned: Whether to list only owned charts.
        Returns:
            List of chart Response objects matching the criteria.
        """
        body: dict[str, Any] = {}
        if visibility is not None or is_owned is not None:
            filter_: dict[str, str | bool] = {}
            if visibility is not None:
                filter_["visibility"] = visibility.upper()
            if is_owned is not None:
                filter_["isOwned"] = is_owned
            body["filter"] = filter_
        return self._list(body=body, limit=limit)
