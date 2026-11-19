import builtins
from collections.abc import Iterable, Sequence

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse, ResponseItems
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsSuccessResponse,
    RequestMessage,
    SuccessResponse,
)
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_trigger import (
    WorkflowTriggerRequest,
    WorkflowTriggerResponse,
    WorkflowTriggerRun,
)


class WorkflowTriggersAPI(CDFResourceAPI[WorkflowTriggerResponse]):
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

    def create(self, items: Sequence[WorkflowTriggerRequest]) -> builtins.list[WorkflowTriggerResponse]:
        """Create or update workflow triggers in CDF.

        Args:
            items: List of WorkflowTriggerRequest objects to create or update.
        Returns:
            List of created/updated WorkflowTriggerResponse objects.
        """
        return self._request_item_response(items, "upsert")

    # This is a duplicate of the create method, included to standardize the API interface.
    def update(self, items: Sequence[WorkflowTriggerRequest]) -> builtins.list[WorkflowTriggerResponse]:
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

    def history(self, item: ExternalId, limit: int | None = 100) -> builtins.list[WorkflowTriggerRun]:
        """Retrieve the times a workflow trigger fired.

        Args:
            item: The workflow trigger.
            limit: Maximum number of runs to return. None returns all.

        Returns:
            The trigger runs.
        """
        if limit is not None and limit <= 0:
            return []
        url = self._make_url(f"/workflows/triggers/{item.external_id}/history")
        page_size = self._method_endpoint_map["list"].item_limit
        results: builtins.list[WorkflowTriggerRun] = []
        cursor: str | None = None
        while True:
            page_limit = page_size if limit is None else min(limit - len(results), page_size)
            params: dict[str, str | int] = {"limit": page_limit}
            if cursor is not None:
                params["cursor"] = cursor
            request = RequestMessage(endpoint_url=url, method="GET", parameters=params)
            response = self._http_client.request_single_retries(request).get_success_or_raise(request)
            page = PagedResponse[WorkflowTriggerRun].model_validate_json(response.body)
            results.extend(page.items)
            cursor = page.next_cursor
            if cursor is None or not page.items or (limit is not None and len(results) >= limit):
                return results

    def _request_per_trigger(
        self,
        items: Sequence[ExternalId],
        path_suffix: str,
        ignore_unknown_ids: bool = False,
    ) -> None:
        for item in items:
            request = RequestMessage(
                endpoint_url=self._make_url(f"/workflows/triggers/{item.external_id}{path_suffix}"),
                method="POST",
            )
            response = self._http_client.request_single_retries(request)
            if isinstance(response, SuccessResponse) or ignore_unknown_ids:
                continue
            _ = response.get_success_or_raise(request)

    def pause(self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False) -> None:
        """Pause workflow triggers.

        When paused, a trigger will not fire until it is resumed. For data modeling
        and records stream triggers, processing continues from the cursor at pause
        time rather than the freshest data. That cursor can become invalid if the
        trigger is paused longer than the stream retention.

        Args:
            items: List of ExternalId objects identifying triggers to pause.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        """
        self._request_per_trigger(items, "/pause", ignore_unknown_ids)

    def resume(self, items: Sequence[ExternalId], ignore_unknown_ids: bool = False) -> None:
        """Resume paused workflow triggers.

        Once resumed, a trigger will fire according to its configuration. For data
        modeling and records stream triggers, processing continues from the cursor
        at pause time rather than the freshest data. That cursor can become invalid
        if the trigger is paused longer than the stream retention.

        Args:
            items: List of ExternalId objects identifying triggers to resume.
            ignore_unknown_ids: Whether to ignore unknown IDs.
        """
        self._request_per_trigger(items, "/resume", ignore_unknown_ids)

    # The list endpoint has no filter parameters, so callers filter triggers, for example by workflow, themselves.
    def paginate(
        self,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[WorkflowTriggerResponse]:
        """Fetch a page of workflow triggers from CDF.

        Args:
            limit: Maximum number of items to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of WorkflowTriggerResponse objects.
        """
        return self._paginate(cursor=cursor, limit=limit)

    def iterate(
        self,
        limit: int | None = 100,
    ) -> Iterable[builtins.list[WorkflowTriggerResponse]]:
        """Iterate over all workflow triggers in CDF.

        Args:
            limit: Maximum number of items to return.

        Returns:
            Iterable of lists of WorkflowTriggerResponse objects.
        """
        return self._iterate(limit=limit)

    def list(
        self,
        limit: int | None = 100,
    ) -> builtins.list[WorkflowTriggerResponse]:
        """List all workflow triggers in CDF.

        Args:
            limit: Maximum number of items to return.

        Returns:
            List of WorkflowTriggerResponse objects.
        """
        return self._list(limit=limit)
