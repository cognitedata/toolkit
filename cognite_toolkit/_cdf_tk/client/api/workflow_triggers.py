from collections.abc import Iterable, Sequence
from typing import Any

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_trigger import (
    WorkflowTriggerRequest,
    WorkflowTriggerResponse,
)


class WorkflowTriggersAPI(CDFResourceAPI[ExternalId, WorkflowTriggerRequest, WorkflowTriggerResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "upsert": Endpoint(method="POST", path="/workflows/triggers", item_limit=1),
                "delete": Endpoint(method="POST", path="/workflows/triggers/delete", item_limit=1),
                "list": Endpoint(method="GET", path="/workflows/triggers", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[WorkflowTriggerResponse]:
        return PagedResponse[WorkflowTriggerResponse].model_validate_json(response.body)

    def _reference_response(self, response: SuccessResponse) -> ResponseItems[ExternalId]:
        return ResponseItems[ExternalId].model_validate_json(response.body)

    def create(self, items: Sequence[WorkflowTriggerRequest]) -> list[WorkflowTriggerResponse]:
        """Create or update workflow triggers in CDF.

        Args:
            items: List of WorkflowTriggerRequest objects to create or update.
        Returns:
            List of created/updated WorkflowTriggerResponse objects.
        """
        return self._request_item_response(items, "upsert")

    # This is a duplicate of the create method, included to standardize the API interface.
    def update(self, items: Sequence[WorkflowTriggerRequest]) -> list[WorkflowTriggerResponse]:
        """Create or update workflow triggers in CDF.

        Args:
            items: List of WorkflowTriggerRequest objects to create or update.
        Returns:
            List of created/updated WorkflowTriggerResponse objects.
        """
        return self.create(items)

    def delete(self, items: Sequence[ExternalId]) -> None:
        """Delete workflow triggers from CDF.

        Args:
            items: List of ExternalId objects to delete.
        """
        self._request_no_response(items, "delete")

    def paginate(
        self,
        workflow_external_id: str | None = None,
        workflow_version: str | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[WorkflowTriggerResponse]:
        """Iterate over all workflow triggers in CDF.

        Args:
            workflow_external_id: Filter by workflow external ID.
            workflow_version: Filter by workflow version.
            limit: Maximum number of items to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of WorkflowTriggerResponse objects.
        """
        params: dict[str, Any] = {}
        if workflow_external_id:
            params["workflowExternalId"] = workflow_external_id
        if workflow_version:
            params["workflowVersion"] = workflow_version

        return self._paginate(
            cursor=cursor,
            limit=limit,
            params=params,
        )

    def iterate(
        self,
        workflow_external_id: str | None = None,
        workflow_version: str | None = None,
        limit: int = 100,
    ) -> Iterable[list[WorkflowTriggerResponse]]:
        """Iterate over all workflow triggers in CDF.

        Args:
            workflow_external_id: Filter by workflow external ID.
            workflow_version: Filter by workflow version.
            limit: Maximum number of items to return per page.

        Returns:
            Iterable of lists of WorkflowTriggerResponse objects.
        """
        params: dict[str, Any] = {}
        if workflow_external_id:
            params["workflowExternalId"] = workflow_external_id
        if workflow_version:
            params["workflowVersion"] = workflow_version

        return self._iterate(
            limit=limit,
            params=params,
        )

    def list(
        self,
        limit: int | None = 100,
    ) -> list[WorkflowTriggerResponse]:
        """List all workflow triggers in CDF.

        Returns:
            List of WorkflowTriggerResponse objects.
        """
        return self._list(limit=limit)
