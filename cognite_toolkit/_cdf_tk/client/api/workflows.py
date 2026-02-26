from collections.abc import Iterable, Sequence

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsSuccessResponse,
    RequestMessage,
    SuccessResponse,
)
from cognite_toolkit._cdf_tk.client.identifiers.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.workflow import WorkflowRequest, WorkflowResponse

from .workflow_triggers import WorkflowTriggersAPI
from .workflow_versions import WorkflowVersionsAPI


class WorkflowsAPI(CDFResourceAPI[WorkflowResponse]):
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

    def retrieve(self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False) -> list[WorkflowResponse]:
        """Retrieve workflows from CDF by external ID.

        Args:
            items: List of ExternalId objects to retrieve.
            ignore_unknown_ids: Whether to ignore unknown IDs.

        Returns:
            List of retrieved WorkflowResponse objects.
        """
        result: list[WorkflowResponse] = []
        endpoint = self._method_endpoint_map["retrieve"]
        for item in items:
            response = self._http_client.request_single_retries(
                RequestMessage(
                    endpoint_url=self._make_url(f"/workflows/{item.external_id}"),
                    method=endpoint.method,
                )
            )
            if isinstance(response, SuccessResponse):
                result.append(WorkflowResponse.model_validate_json(response.body))
            elif ignore_unknown_ids:
                continue
            else:
                _ = response.get_success_or_raise()
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
