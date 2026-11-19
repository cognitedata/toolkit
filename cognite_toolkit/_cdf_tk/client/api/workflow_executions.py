"""Workflow Executions API for running and monitoring CDF workflow executions.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Workflow-executions
"""

import builtins
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
from cognite_toolkit._cdf_tk.client.request_classes.filters import WorkflowExecutionFilter
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_execution import (
    WorkflowExecutionDetailedResponse,
    WorkflowExecutionResponse,
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
    ) -> builtins.list[T_ExecutionResponse]:
        result: builtins.list[T_ExecutionResponse] = []
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
    ) -> builtins.list[WorkflowExecutionDetailedResponse]:
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
    ) -> builtins.list[WorkflowExecutionResponse]:
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
    ) -> builtins.list[WorkflowExecutionResponse]:
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

    @staticmethod
    def _create_list_body(filter: WorkflowExecutionFilter | None) -> dict[str, Any] | None:
        if filter is None:
            return None
        dumped = filter.dump()
        return {"filter": dumped} if dumped else None

    def paginate(
        self,
        filter: WorkflowExecutionFilter | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> PagedResponse[WorkflowExecutionResponse]:
        """Fetch a page of workflow executions from CDF.

        Args:
            filter: Filter by workflow (and optionally version), creation time, and status.
            limit: Maximum number of items to return.
            cursor: Cursor for pagination.

        Returns:
            PagedResponse of WorkflowExecution objects.
        """
        return self._paginate(cursor=cursor, limit=limit, body=self._create_list_body(filter))

    def iterate(
        self,
        filter: WorkflowExecutionFilter | None = None,
        limit: int | None = 100,
    ) -> Iterable[builtins.list[WorkflowExecutionResponse]]:
        """Iterate over workflow executions in CDF.

        Args:
            filter: Filter by workflow (and optionally version), creation time, and status.
            limit: Maximum number of items to return.

        Returns:
            Iterable of lists of WorkflowExecution objects.
        """
        return self._iterate(limit=limit, body=self._create_list_body(filter))

    def list(
        self,
        filter: WorkflowExecutionFilter | None = None,
        limit: int | None = 100,
    ) -> builtins.list[WorkflowExecutionResponse]:
        """List workflow executions in CDF.

        Args:
            filter: Filter by workflow (and optionally version), creation time, and status.
            limit: Maximum number of items to return.

        Returns:
            List of WorkflowExecution objects.
        """
        return self._list(limit=limit, body=self._create_list_body(filter))
