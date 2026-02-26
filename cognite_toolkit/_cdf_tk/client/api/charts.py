from collections.abc import Sequence
from typing import Any

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.chart import (
    ChartRequest,
    ChartResponse,
    Visibility,
)


class ChartsAPI(CDFResourceAPI[ChartResponse]):
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

    def list(self, visibility: Visibility | None = None, is_owned: bool | None = None) -> list[ChartResponse]:
        """List charts based on visibility and ownership.

        Args:
            visibility: Visibility of the charts to list, either 'PUBLIC' or 'PRIVATE'.
            is_owned: Whether to list only owned charts.
        Returns:
            List of chart Response objects matching the criteria.
        """
        filter_params: dict[str, Any] = {}
        if visibility is not None:
            filter_params["visibility"] = visibility
        if is_owned is not None:
            filter_params["isOwned"] = is_owned
        body: dict[str, Any] = {}
        if filter_params:
            body["filter"] = filter_params
        endpoint = self._method_endpoint_map["list"]
        # Note that even though the internal docs specify that limit is supported for this endpoint,
        # you get: "Encountered an unknown key 'limit' at offset 50 at path: $" if you pass it.
        response = self._http_client.request_single_retries(
            RequestMessage(
                endpoint_url=self._make_url(endpoint.path),
                method=endpoint.method,
                body_content=body,
            )
        ).get_success_or_raise()
        return self._validate_page_response(response).items
