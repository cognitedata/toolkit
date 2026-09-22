"""Workflow Executions API for running and monitoring CDF workflow executions.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Workflow-executions
"""

from collections.abc import Iterable, Sequence
from typing import Any, Literal, TypeVar

from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.client._types import Metadata
from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsSuccessResponse,
    RequestMessage,
    SuccessResponse,
)
from cognite_toolkit._cdf_tk.client.identifiers import WorkflowExecutionId, WorkflowVersionId
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_execution import (
    WorkflowExecutionDetailedResponse,
    WorkflowExecutionResponse,
    WorkflowExecutionStatus,
)

T_ExecutionResponse = TypeVar("T_ExecutionResponse", bound=BaseModelObject)


class WorkflowExecutionsAPI(CDFResourceAPI[WorkflowExecutionResponse]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "retrieve": Endpoint(method="GET", path="/workflows/executions/{executionId}", item_limit=1),
                "list": Endpoint(method="POST", path="/workflows/executions/list", item_limit=1000),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[WorkflowExecutionResponse]:
        return PagedResponse[WorkflowExecutionResponse].model_validate_json(response.body)

    def _request_per_execution(
        self,
        items: Sequence[WorkflowExecutionId],
        path_suffix: str,
        response_cls: type[T_ExecutionResponse],
        ignore_unknown_ids: bool = False,
        method: Literal["GET", "POST"] = "POST",
        body_content: dict[str, JsonValue] | None = None,
    ) -> list[T_ExecutionResponse]:
        result: list[T_ExecutionResponse] = []
        for item in items:
            request = RequestMessage(
                endpoint_url=self._make_url(f"/workflows/executions/{item.id}{path_suffix}"),
                method=method,
                body_content=body_content,
            )
            response = self._http_client.request_single_retries(request)
            if isinstance(response, SuccessResponse):
                result.append(response_cls.model_validate_json(response.body))
            elif ignore_unknown_ids:
                continue
            else:
                _ = response.get_success_or_raise(request)
        return result

    def run(
        self,
        item: WorkflowVersionId,
        nonce: str,
        input: dict[str, JsonValue] | None = None,
        metadata: Metadata | None = None,
    ) -> WorkflowExecutionResponse:
        """Start an execution of a specific version of a workflow.

        Args:
            item: Workflow version to run.
            nonce: Session nonce used to authenticate the execution.
            input: Input data to the workflow.
            metadata: Custom, application-specific metadata.

        Returns:
            The created WorkflowExecution.
        """
        body: dict[str, Any] = {"authentication": {"nonce": nonce}}
        if input is not None:
            body["input"] = input
        if metadata is not None:
            body["metadata"] = metadata
        request = RequestMessage(
            endpoint_url=self._make_url(f"/workflows/{item.workflow_external_id}/versions/{item.version}/run"),
            method="POST",
            body_content=body,
        )
        response = self._http_client.request_single_retries(request).get_success_or_raise(request)
        return WorkflowExecutionResponse.model_validate_json(response.body)

    def retrieve(
        self, items: Sequence[WorkflowExecutionId], ignore_unknown_ids: bool = False
    ) -> list[WorkflowExecutionDetailedResponse]:
        """Retrieve detailed workflow executions from CDF.

        Args:
            items: List of WorkflowExecutionId objects to retrieve.
            ignore_unknown_ids: Whether to ignore unknown IDs.

        Returns:
            List of retrieved WorkflowExecutionDetailed objects.
        """
        return self._request_per_execution(
            items,
            path_suffix="",
            response_cls=WorkflowExecutionDetailedResponse,
            ignore_unknown_ids=ignore_unknown_ids,
            method="GET",
        )

    def cancel(
        self,
        items: Sequence[WorkflowExecutionId],
        reason: str | None = None,
        ignore_unknown_ids: bool = False,
    ) -> list[WorkflowExecutionResponse]:
        """Cancel workflow executions.

        Stops the specified executions from starting new workflow tasks and sets the
        workflow execution status to TERMINATED. Already running tasks will be marked
        as CANCELED. Actions taken by canceled tasks are not stopped and must be
        canceled separately if desired.

        Args:
            items: List of WorkflowExecutionId objects to cancel.
            reason: Human-readable reason for the cancellation. Defaults to the API
                default of "cancelled" when omitted.
            ignore_unknown_ids: Whether to ignore unknown IDs.

        Returns:
            List of updated WorkflowExecution objects.
        """
        body: dict[str, JsonValue] | None = None
        if reason is not None:
            body = {"reason": reason}
        return self._request_per_execution(
            items,
            path_suffix="/cancel",
            response_cls=WorkflowExecutionResponse,
            ignore_unknown_ids=ignore_unknown_ids,
            body_content=body,
        )

    def retry(
        self,
        items: Sequence[WorkflowExecutionId],
        nonce: str | None = None,
        ignore_unknown_ids: bool = False,
    ) -> list[WorkflowExecutionResponse]:
        """Retry previously failed, timed out, or terminated workflow executions.

        Args:
            items: List of WorkflowExecutionId objects to retry.
            nonce: Optional session nonce used to authenticate the retry.
            ignore_unknown_ids: Whether to ignore unknown IDs.

        Returns:
            List of updated WorkflowExecution objects.
        """
        body: dict[str, JsonValue] | None = None
        if nonce is not None:
            body = {"authentication": {"nonce": nonce}}
        return self._request_per_execution(
            items,
            path_suffix="/retry",
            response_cls=WorkflowExecutionResponse,
            ignore_unknown_ids=ignore_unknown_ids,
            body_content=body,
        )

    def _create_list_body(
        self,
        workflow_version_ids: Sequence[WorkflowVersionId] | None = None,
        created_time_start: int | None = None,
        created_time_end: int | None = None,
        statuses: Sequence[WorkflowExecutionStatus | str] | None = None,
    ) -> dict[str, Any] | None:
        filter_body: dict[str, Any] = {}
        if workflow_version_ids:
            filter_body["workflowFilters"] = [
                {"externalId": item.workflow_external_id, "version": item.version} for item in workflow_version_ids
            ]
        if created_time_start is not None:
            filter_body["createdTimeStart"] = created_time_start
        if created_time_end is not None:
            filter_body["createdTimeEnd"] = created_time_end
        if statuses:
            filter_body["status"] = list(statuses)
        return {"filter": filter_body} if filter_body else None

    def paginate(
        self,
        workflow_version_ids: Sequence[WorkflowVersionId] | None = None,
        created_time_start: int | None = None,
        created_time_end: int | None = None,
        statuses: Sequence[WorkflowExecutionStatus | str] | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[WorkflowExecutionResponse]:
        """Fetch a page of workflow executions from CDF.

        Args:
            workflow_version_ids: Filter by workflow version identifiers.
            created_time_start: Filter out executions created before this time (ms since epoch).
            created_time_end: Filter out executions created after this time (ms since epoch).
            statuses: Filter by workflow execution status.
            limit: Maximum number of items to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of WorkflowExecution objects.
        """
        return self._paginate(
            cursor=cursor,
            limit=limit,
            body=self._create_list_body(
                workflow_version_ids,
                created_time_start,
                created_time_end,
                statuses,
            ),
        )

    def iterate(
        self,
        workflow_version_ids: Sequence[WorkflowVersionId] | None = None,
        created_time_start: int | None = None,
        created_time_end: int | None = None,
        statuses: Sequence[WorkflowExecutionStatus | str] | None = None,
        limit: int | None = 100,
    ) -> Iterable[list[WorkflowExecutionResponse]]:
        """Iterate over workflow executions in CDF.

        Args:
            workflow_version_ids: Filter by workflow version identifiers.
            created_time_start: Filter out executions created before this time (ms since epoch).
            created_time_end: Filter out executions created after this time (ms since epoch).
            statuses: Filter by workflow execution status.
            limit: Maximum number of items to return per page.

        Returns:
            Iterable of lists of WorkflowExecution objects.
        """
        return self._iterate(
            limit=limit,
            body=self._create_list_body(
                workflow_version_ids,
                created_time_start,
                created_time_end,
                statuses,
            ),
        )

    def list(
        self,
        workflow_version_ids: Sequence[WorkflowVersionId] | None = None,
        created_time_start: int | None = None,
        created_time_end: int | None = None,
        statuses: Sequence[WorkflowExecutionStatus | str] | None = None,
        limit: int | None = 100,
    ) -> list[WorkflowExecutionResponse]:
        """List workflow executions in CDF.

        Args:
            workflow_version_ids: Filter by workflow version identifiers.
            created_time_start: Filter out executions created before this time (ms since epoch).
            created_time_end: Filter out executions created after this time (ms since epoch).
            statuses: Filter by workflow execution status.
            limit: Maximum number of items to return.

        Returns:
            List of WorkflowExecution objects.
        """
        return self._list(
            limit=limit,
            body=self._create_list_body(
                workflow_version_ids,
                created_time_start,
                created_time_end,
                statuses,
            ),
        )
