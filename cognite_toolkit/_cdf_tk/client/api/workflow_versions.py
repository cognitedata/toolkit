import builtins
from collections.abc import Iterable, Sequence
from typing import Any

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsSuccessResponse,
    RequestMessage,
    SuccessResponse,
)
from cognite_toolkit._cdf_tk.client.identifiers import WorkflowVersionId
from cognite_toolkit._cdf_tk.client.request_classes.filters import WorkflowFilter
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_version import (
    WorkflowVersionRequest,
    WorkflowVersionResponse,
)


class WorkflowVersionsAPI(CDFResourceAPI[WorkflowVersionResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "upsert": Endpoint(method="POST", path="/workflows/versions", item_limit=1),
                "retrieve": Endpoint(
                    method="GET", path="/workflows/{workflowExternalId}/versions/{version}", item_limit=1
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

    def create(self, items: Sequence[WorkflowVersionRequest]) -> builtins.list[WorkflowVersionResponse]:
        """Create or update workflow versions in CDF.

        Args:
            items: List of WorkflowVersionRequest objects to create or update.
        Returns:
            List of created/updated WorkflowVersionResponse objects.
        """
        return self._request_item_response(items, "upsert")

    # This is a duplicate of the create method, included to standardize the API interface.
    def update(self, items: Sequence[WorkflowVersionRequest]) -> builtins.list[WorkflowVersionResponse]:
        """Create or update workflow versions in CDF.

        Args:
            items: List of WorkflowVersionRequest objects to create or update.
        Returns:
            List of created/updated WorkflowVersionResponse objects.
        """
        return self.create(items)

    def retrieve(
        self, items: Sequence[WorkflowVersionId], ignore_unknown_ids: bool = False
    ) -> builtins.list[WorkflowVersionResponse]:
        """Retrieve workflow versions from CDF.

        Args:
            items: List of WorkflowVersionId objects to retrieve.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        Returns:
            List of retrieved WorkflowVersionResponse objects.
        """
        result: builtins.list[WorkflowVersionResponse] = []
        endpoint = self._method_endpoint_map["retrieve"]
        for item in items:
            request = RequestMessage(
                endpoint_url=self._make_url(f"/workflows/{item.workflow_external_id}/versions/{item.version}"),
                method=endpoint.method,
            )
            response = self._http_client.request_single_retries(request)
            if isinstance(response, SuccessResponse):
                result.append(WorkflowVersionResponse.model_validate_json(response.body))
            elif ignore_unknown_ids:
                continue
            else:
                _ = response.get_success_or_raise(request)
        return result

    def delete(self, items: Sequence[WorkflowVersionId]) -> None:
        """Delete workflow versions from CDF.

        Args:
            items: List of WorkflowVersionId objects to delete.
        """
        self._request_no_response(items, "delete")

    @staticmethod
    def _create_list_body(filter: WorkflowFilter | None) -> dict[str, Any] | None:
        if filter is None:
            return None
        dumped = filter.dump()
        return {"filter": dumped} if dumped else None

    def paginate(
        self,
        filter: WorkflowFilter | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[WorkflowVersionResponse]:
        """Fetch a page of workflow versions from CDF.

        Args:
            filter: Only return versions of these workflows, optionally limited to given versions.
            limit: Maximum number of items to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of WorkflowVersionResponse objects.
        """
        return self._paginate(cursor=cursor, limit=limit, body=self._create_list_body(filter))

    def iterate(
        self,
        filter: WorkflowFilter | None = None,
        limit: int | None = 100,
    ) -> Iterable[builtins.list[WorkflowVersionResponse]]:
        """Iterate over workflow versions in CDF.

        Args:
            filter: Only return versions of these workflows, optionally limited to given versions.
            limit: Maximum number of items to return.

        Returns:
            Iterable of lists of WorkflowVersionResponse objects.
        """
        return self._iterate(limit=limit, body=self._create_list_body(filter))

    def list(
        self,
        filter: WorkflowFilter | None = None,
        limit: int | None = 100,
    ) -> builtins.list[WorkflowVersionResponse]:
        """List workflow versions in CDF.

        Args:
            filter: Only return versions of these workflows, optionally limited to given versions.
            limit: Maximum number of items to return.

        Returns:
            List of WorkflowVersionResponse objects.
        """
        return self._list(limit=limit, body=self._create_list_body(filter))
