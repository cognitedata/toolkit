from typing import Literal, TypeAlias

from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.client._types import Metadata
from cognite_toolkit._cdf_tk.client.identifiers import WorkflowExecutionId, WorkflowVersionId
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_version import WorkflowDefinition

WorkflowExecutionStatus: TypeAlias = Literal["RUNNING", "COMPLETED", "FAILED", "TERMINATED", "TIMED_OUT"]
WorkflowTaskExecutionStatus: TypeAlias = Literal[
    "SCHEDULED",
    "IN_PROGRESS",
    "CANCELED",
    "FAILED",
    "FAILED_WITH_TERMINAL_ERROR",
    "COMPLETED",
    "COMPLETED_WITH_ERRORS",
    "TIMED_OUT",
    "SKIPPED",
]


class WorkflowExecutionResponse(BaseModelObject):
    id: str
    workflow_external_id: str
    version: str | None = None
    status: WorkflowExecutionStatus | str
    engine_execution_id: str | None = None
    created_time: int
    start_time: int | None = None
    end_time: int | None = None
    reason_for_incompletion: str | None = None
    metadata: Metadata | None = None

    def as_id(self) -> WorkflowExecutionId:
        return WorkflowExecutionId(id=self.id)

    def as_workflow_id(self) -> WorkflowVersionId:
        if self.version is None:
            raise ValueError("Cannot create WorkflowVersionId: version is None")
        return WorkflowVersionId(workflow_external_id=self.workflow_external_id, version=self.version)


class WorkflowTaskExecution(BaseModelObject):
    id: str
    external_id: str
    parent_task_external_id: str | None = None
    status: WorkflowTaskExecutionStatus | str
    task_type: str | None = None
    start_time: int | None = None
    end_time: int | None = None
    input: JsonValue | None = None
    output: JsonValue | None = None
    reason_for_incompletion: str | None = None


class WorkflowExecutionDetailedResponse(WorkflowExecutionResponse):
    workflow_definition: WorkflowDefinition
    executed_tasks: list[WorkflowTaskExecution]
    input: JsonValue | None = None
