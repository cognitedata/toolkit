from typing import Literal

from cognite.client.data_classes import WorkflowVersionUpsert
from pydantic import Field, JsonValue

from .base import BaseModelResource, ToolkitResource


class TaskId(BaseModelResource):
    external_id: str = Field(
        max_length=255, description="The external ID provided by the client. Must be unique for the resource type."
    )


class TaskDefinition(BaseModelResource):
    external_id: str = Field(
        max_length=255,
        description="Identifier for the task. Must be unique within the version. No trailing or leading whitespace and no null characters allowed.",
    )
    type: Literal["function", "transformation", "cdf", "dynamic", "subworkflow", "simulation"]
    name: str | None = Field(
        default=None,
        max_length=255,
        description="Readable name meant for use in UIs",
    )
    description: str | None = Field(
        default=None,
        max_length=500,
        description="Description of the intention of the task",
    )
    parameters: JsonValue = Field()
    retries: int = Field(
        3,
        ge=0,
        le=10,
        description="Number of times to retry the task if it fails. If set to 0, the task will not be retried. The behavior for timeouts and retries is defined by the onFailure parameter, refer to it for more information.",
    )
    timeout: int = Field(
        3600,
        ge=60,
        le=86400,
        description="TTimeout in seconds. After this time, the task will be marked as TIMED_OUT. By default, the task won't be retried upon timeout. Use the onFailure parameter to change this behavior.",
    )
    on_failure: Literal["abortWorkflow", "skipTask"] = Field(
        "abortWorkflow",
        description="Defines the behavior when a task fails. If set to abortWorkflow, the entire workflow will be marked as FAILED when the task fails. If set to skipTask, the task will be marked as FAILED, but the workflow will continue executing subsequent tasks that are not dependent on the failed task.",
    )
    depends_on: list[TaskId] | None = Field(
        None,
        description="The tasks that must be completed before this task can be executed.",
        max_length=100,
    )


class WorkflowDefinition(BaseModelResource):
    description: str | None = Field(
        default=None,
        max_length=500,
        description="The description of the workflow version.",
    )
    tasks: list[TaskDefinition]


class WorkflowVersionYAML(ToolkitResource):
    _cdf_resource = WorkflowVersionUpsert
    workflow_external_id: str = Field(
        max_length=255,
        description="Identifier for a workflow. Must be unique for the project. No trailing or leading whitespace and no null characters allowed.",
    )
    version: str = Field(
        max_length=255,
        description="Identifier for a version. Must be unique for the workflow. No trailing or leading whitespace and no null characters allowed.",
    )
    workflow_definition: WorkflowDefinition
