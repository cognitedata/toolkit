from collections.abc import Sequence

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.data_classes.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.data_classes.workflow import WorkflowRequest, WorkflowResponse
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, SuccessResponse2


class WorkflowsAPI(CDFResourceAPI[ExternalId, WorkflowRequest, WorkflowResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "create": Endpoint(method="POST", path="/workflows", item_limit=100, concurrency_max_workers=1),
                "delete": Endpoint(method="POST", path="/workflows/delete", item_limit=100, concurrency_max_workers=1),
                "list": Endpoint(method="GET", path="/workflows", item_limit=100),
            },
        )

    def _page_response(self, response: SuccessResponse2) -> PagedResponse[WorkflowResponse]:
        return PagedResponse[WorkflowResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse2) -> ResponseItems[ExternalId]:
        return ResponseItems[ExternalId].model_validate_json(response.body)

    def upsert(self, items: Sequence[WorkflowRequest]) -> list[WorkflowResponse]:
        """Create or update workflows in CDF.

        Args:
            items: List of WorkflowRequest objects to create or update.
        Returns:
            List of created/updated WorkflowResponse objects.
        """
        return self._request_item_response(items, "create")

    def retrieve(self, external_id: str) -> WorkflowResponse | None:
        """Retrieve a workflow from CDF by external ID.

        Args:
            external_id: The external ID of the workflow to retrieve.
        Returns:
            The retrieved WorkflowResponse object, or None if not found.
        """
        from cognite_toolkit._cdf_tk.client.http_client import RequestMessage2

        request = RequestMessage2(
            endpoint_url=self._make_url(f"/workflows/{external_id}"),
            method="GET",
        )
        result = self._http_client.request_single_retries(request)
        response = result.get_success_or_raise()
        # Workflow retrieve returns single item directly, not in items array
        from cognite_toolkit._cdf_tk.client.data_classes.workflow import WorkflowResponse

        return WorkflowResponse.model_validate_json(response.body)

    def delete(self, items: Sequence[ExternalId]) -> None:
        """Delete workflows from CDF.

        Args:
            items: List of ExternalId objects to delete.
        """
        self._request_no_response(items, "delete")

    def iterate(
        self,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[WorkflowResponse]:
        """Iterate over all workflows in CDF.

        Args:
            limit: Maximum number of items to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of WorkflowResponse objects.
        """
        return self._iterate(
            cursor=cursor,
            limit=limit,
        )

    def list(
        self,
        limit: int | None = 100,
    ) -> list[WorkflowResponse]:
        """List all workflows in CDF.

        Returns:
            List of WorkflowResponse objects.
        """
        return self._list(limit=limit)
