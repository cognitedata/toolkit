from collections.abc import Sequence
from typing import Any

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, InternalId
from cognite_toolkit._cdf_tk.client.request_classes.filters import ChartMonitorJobFilter
from cognite_toolkit._cdf_tk.client.resource_classes.chart_monitoring_job import (
    ChartMonitoringJobRequest,
    ChartMonitoringJobResponse,
)


class ChartMonitoringJobAPI(CDFResourceAPI[ChartMonitoringJobResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/monitoringtasks", item_limit=1),
                "delete": Endpoint(method="POST", path="/monitoringtasks/delete", item_limit=1),
                "upsert": Endpoint(method="POST", path="monitoringtasks/upsert", item_limit=1),
                "retrieve": Endpoint(method="POST", path="/monitoringtasks/byids", item_limit=1),
                "update": Endpoint(method="POST", path="/monitoringtasks/update", item_limit=1),
                "list": Endpoint(method="POST", path="/monitoringtasks/list", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[ChartMonitoringJobResponse]:
        return PagedResponse[ChartMonitoringJobResponse].model_validate_json(response.body)

    def create(self, items: Sequence[ChartMonitoringJobRequest]) -> list[ChartMonitoringJobResponse]:
        """Create monitoring tasks in CDF.

        Args:
            items: Monitoring job request objects to create.
        Returns:
            Created monitoring job response objects.
        """
        return self._request_item_response(items, "create")

    def retrieve(self, items: Sequence[InternalId | ExternalId]) -> list[ChartMonitoringJobResponse]:
        """Retrieve monitoring tasks by internal or external ID.

        Args:
            items: Identifiers to retrieve.
        Returns:
            Retrieved monitoring job response objects.
        """
        return self._request_item_response(items, "retrieve")

    def delete(self, items: Sequence[InternalId | ExternalId]) -> None:
        """Delete monitoring tasks by internal or external ID.

        Args:
            items: Identifiers to delete.
        """
        self._request_no_response(items, "delete")

    def update(self, items: Sequence[ChartMonitoringJobRequest]) -> list[ChartMonitoringJobResponse]:
        """Update monitoring tasks.

        Args:
            items: Monitoring job request objects to update.
        Returns:
            Updated monitoring job response objects.
        """
        return self._request_item_response(items, "update")

    def upsert(self, items: Sequence[ChartMonitoringJobRequest]) -> list[ChartMonitoringJobResponse]:
        """Upsert monitoring tasks.

        Args:
            items: Monitoring job request objects to upsert.
        Returns:
            Upserted monitoring job response objects.
        """
        return self._request_item_response(items, "upsert")

    def list(self, filter_: ChartMonitorJobFilter | None = None, limit: int = 100) -> list[ChartMonitoringJobResponse]:
        """List monitoring tasks.

        Args:
            filter_: Optional filter to apply when listing monitoring tasks.
            limit: Maximum number of monitoring tasks to return.

        Returns:
            Monitoring job response objects matching the request.
        """
        body: dict[str, Any] = {"limit": limit}
        if filter_ is not None:
            body["filter"] = filter_.dump()
        endpoint = self._method_endpoint_map["list"]
        request = RequestMessage(
            endpoint_url=self._http_client.config.create_api_url(endpoint.path),
            method=endpoint.method,
            body_content=body,
        )
        response = self._http_client.request_single_retries(request).get_success_or_raise(request)
        return self._validate_page_response(response).items
