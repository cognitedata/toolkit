from collections.abc import Iterable, Sequence

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.workflow import WorkflowRequest, WorkflowResponse

from .workflow_triggers import WorkflowTriggersAPI
from .workflow_versions import WorkflowVersionsAPI


class WorkflowsAPI(CDFResourceAPI[ExternalId, WorkflowRequest, WorkflowResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "upsert": Endpoint(method="POST", path="/workflows", item_limit=1),
                "retrieve": Endpoint(method="GET", path="/workflows/{workflowExternalId}", item_limit=1),
                "delete": Endpoint(method="POST", path="/workflows/delete", item_limit=100),
                "list": Endpoint(method="GET", path="/workflows", item_limit=100),
            },
        )
        self.versions = WorkflowVersionsAPI(http_client)
        self.triggers = WorkflowTriggersAPI(http_client)

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[WorkflowResponse]:
        return PagedResponse[WorkflowResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse) -> ResponseItems[ExternalId]:
        return ResponseItems[ExternalId].model_validate_json(response.body)

    def create(self, items: Sequence[WorkflowRequest]) -> list[WorkflowResponse]:
        """Create or update workflows in CDF.

        Args:
            items: List of WorkflowRequest objects to create or update.
        Returns:
            List of created/updated WorkflowResponse objects.
        """
        return self._request_item_response(items, "upsert")

    # This is a duplicate of the create method, included to standardize the API interface.
    def update(self, items: Sequence[WorkflowRequest]) -> list[WorkflowResponse]:
        """Create or update workflows in CDF.

        Args:
            items: List of WorkflowRequest objects to create or update.
        Returns:
            List of created/updated WorkflowResponse objects.
        """
        return self.create(items)

    def retrieve(self, items: Sequence[ExternalId]) -> list[WorkflowResponse]:
        """Retrieve a workflow from CDF by external ID.

        Args:
            items: List of ExternalId objects to retrieve.

        Returns:
            The retrieved WorkflowResponse object, or None if not found.
        """
        result: list[WorkflowResponse] = []
        for item in items:
            endpoint = f"/workflows/{item.external_id}"
            retrieved = self._request_item_response([item], "retrieve", endpoint=endpoint)
            result.extend(retrieved)
        return result

    def delete(self, items: Sequence[ExternalId]) -> None:
        """Delete workflows from CDF.

        Args:
            items: List of ExternalId objects to delete.
        """
        self._request_no_response(items, "delete")

    def paginate(
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
        return self._paginate(cursor=cursor, limit=limit)

    def iterate(
        self,
        limit: int = 100,
    ) -> Iterable[list[WorkflowResponse]]:
        """Iterate over all workflows in CDF.

        Args:
            limit: Maximum number of items to return per page.

        Returns:
            Iterable of lists of WorkflowResponse objects.
        """
        return self._iterate(limit=limit)

    def list(
        self,
        limit: int | None = 100,
    ) -> list[WorkflowResponse]:
        """List all workflows in CDF.

        Returns:
            List of WorkflowResponse objects.
        """
        return self._list(limit=limit)
