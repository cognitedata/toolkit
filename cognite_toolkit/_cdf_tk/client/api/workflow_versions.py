from collections.abc import Iterable, Sequence
from typing import Any

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import WorkflowVersionId
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_version import (
    WorkflowVersionRequest,
    WorkflowVersionResponse,
)


class WorkflowVersionsAPI(CDFResourceAPI[WorkflowVersionId, WorkflowVersionRequest, WorkflowVersionResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "upsert": Endpoint(method="POST", path="/workflows/versions", item_limit=1),
                "retrieve": Endpoint(
                    method="GET", path="/workflow/{workflowExternalId}/versions/{version}", item_limit=1
                ),
                "delete": Endpoint(method="POST", path="/workflows/versions/delete", item_limit=100),
                "list": Endpoint(method="POST", path="/workflows/versions/list", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[WorkflowVersionResponse]:
        return PagedResponse[WorkflowVersionResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse) -> ResponseItems[WorkflowVersionId]:
        return ResponseItems[WorkflowVersionId].model_validate_json(response.body)

    def create(self, items: Sequence[WorkflowVersionRequest]) -> list[WorkflowVersionResponse]:
        """Create or update workflow versions in CDF.

        Args:
            items: List of WorkflowVersionRequest objects to create or update.
        Returns:
            List of created/updated WorkflowVersionResponse objects.
        """
        return self._request_item_response(items, "upsert")

    # This is a duplicate of the create method, included to standardize the API interface.
    def update(self, items: Sequence[WorkflowVersionRequest]) -> list[WorkflowVersionResponse]:
        """Create or update workflow versions in CDF.

        Args:
            items: List of WorkflowVersionRequest objects to create or update.
        Returns:
            List of created/updated WorkflowVersionResponse objects.
        """
        return self.create(items)

    def retrieve(self, items: Sequence[WorkflowVersionId]) -> list[WorkflowVersionResponse]:
        """Retrieve workflow versions from CDF.

        Args:
            items: List of WorkflowVersionId objects to retrieve.
        Returns:
            List of retrieved WorkflowVersionResponse objects.
        """
        result: list[WorkflowVersionResponse] = []
        for item in items:
            endpoint = f"/workflows/{item.workflow_external_id}/versions/{item.version}"
            retrieved = self._request_item_response([item], "retrieve", endpoint=endpoint)
            result.extend(retrieved)
        return result

    def delete(self, items: Sequence[WorkflowVersionId]) -> None:
        """Delete workflow versions from CDF.

        Args:
            items: List of WorkflowVersionId objects to delete.
        """
        self._request_no_response(items, "delete")

    def paginate(
        self,
        workflow_external_id: str | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[WorkflowVersionResponse]:
        """Iterate over all workflow versions in CDF.

        Args:
            workflow_external_id: Filter by workflow external ID.
            limit: Maximum number of items to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of WorkflowVersionResponse objects.
        """
        body: dict[str, Any] = {}
        if workflow_external_id:
            body["workflowExternalId"] = workflow_external_id

        return self._paginate(
            cursor=cursor,
            limit=limit,
            body=body,
        )

    def iterate(
        self,
        workflow_external_id: str | None = None,
        limit: int = 100,
    ) -> Iterable[list[WorkflowVersionResponse]]:
        """Iterate over all workflow versions in CDF.

        Args:
            workflow_external_id: Filter by workflow external ID.
            limit: Maximum number of items to return per page.

        Returns:
            Iterable of lists of WorkflowVersionResponse objects.
        """
        body: dict[str, Any] = {}
        if workflow_external_id:
            body["workflowExternalId"] = workflow_external_id

        return self._iterate(
            limit=limit,
            body=body,
        )

    def list(
        self,
        limit: int | None = 100,
    ) -> list[WorkflowVersionResponse]:
        """List all workflow versions in CDF.

        Returns:
            List of WorkflowVersionResponse objects.
        """
        return self._list(limit=limit)
