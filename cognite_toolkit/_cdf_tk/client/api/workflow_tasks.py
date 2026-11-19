from typing import Literal

from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_execution import WorkflowTaskExecution


class WorkflowTasksAPI(CDFResourceAPI[WorkflowTaskExecution]):
    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "update": Endpoint(method="POST", path="/workflows/tasks/{taskId}/update", item_limit=1),
            },
        )

    def _validate_page_response(
        self, response: SuccessResponse | ItemsSuccessResponse
    ) -> PagedResponse[WorkflowTaskExecution]:
        raise NotImplementedError("The task endpoints return a single task execution, not a page of items.")

    def update(
        self,
        task_id: str,
        status: Literal["COMPLETED", "FAILED", "FAILED_WITH_TERMINAL_ERROR"],
        output: dict[str, JsonValue] | None = None,
    ) -> WorkflowTaskExecution:
        """`Complete or fail a task. <https://api-docs.cognite.com/20230101/tag/Tasks/operation/UpdateTaskStatus>`_

        Function tasks with ``isAsyncComplete`` set stay in progress until they are updated with this method.

        Args:
            task_id: The ID of the task execution.
            status: The new status of the task.
            output: The output of the task, available to the tasks that depend on it.

        Returns:
            The updated task execution.
        """
        body: dict[str, JsonValue] = {"status": status}
        if output is not None:
            body["output"] = output
        endpoint = self._method_endpoint_map["update"]
        request = RequestMessage(
            endpoint_url=self._make_url(endpoint.path.format(taskId=task_id)),
            method=endpoint.method,
            body_content=body,
        )
        response = self._http_client.request_single_retries(request).get_success_or_raise(request)
        return WorkflowTaskExecution.model_validate_json(response.body)
