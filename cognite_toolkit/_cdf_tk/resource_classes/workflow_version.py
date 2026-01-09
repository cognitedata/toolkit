from abc import ABC
from typing import Annotated, Literal

from cognite.client.data_classes import WorkflowVersionUpsert
from pydantic import Field, JsonValue

from .base import BaseModelResource, ToolkitResource


class WorkflowVersionId(BaseModelResource):
    workflow_external_id: str = Field(
        max_length=255,
        description="Identifier for a workflow. Must be unique for the project. No trailing or leading whitespace and no null characters allowed.",
    )
    version: str = Field(
        max_length=255,
        description="Identifier for a version. Must be unique for the workflow. No trailing or leading whitespace and no null characters allowed.",
    )


class CogniteFunctionRef(BaseModelResource):
    external_id: str = Field(
        description="The external id of the Cognite Function in the project. This can be either a function external ID or a reference like ${myTaskExternalId.output.someKey}"
    )
    data: str | JsonValue | None = Field(
        None, description="Input data that will be passed to the Cognite Function. Limited to 100KB in size."
    )


class FunctionTaskParameters(BaseModelResource):
    function: CogniteFunctionRef
    is_async_complete: bool = Field(
        False, description="Defines if the execution of the task should be completed asynchronously."
    )


class TransformationRef(BaseModelResource):
    external_id: str = Field(
        description="The external id of the Transformation in the project. This can be either a transformation external ID or a reference like ${myTaskExternalId.output.someKey}"
    )
    concurrency_policy: Literal["fail", "waitForCurrent", "restartAfterCurrent"] = Field(
        "fail",
        description="""Determines the behavior of the task if the Transformation is already running.

fail: The task fails if another instance of the Transformation is currently running.
waitForCurrent: The task will pause and wait for the already running Transformation to complete. Once completed, the task is completed. This mode is useful for preventing redundant Transformation runs.
restartAfterCurrent: The task waits for the ongoing Transformation to finish. After completion, the task restarts the Transformation. This mode ensures that the most recent data can be used by following tasks.""",
    )
    use_transformation_credentials: bool = Field(
        False,
        description="If set to true, the transformation will run using the client credentials configured on the transformation. If set to false, the transformation will run using the client credentials used to trigger the workflow.",
    )


class TransformationTaskParameters(BaseModelResource):
    transformation: TransformationRef


class CDFRequest(BaseModelResource):
    resource_path: str = Field(
        description="The path of the request. The path should be prefixed by {cluster}.cognitedata.com/api/v1/project/{project} based on the relevant cluster and project. It can also contain references like ${myTaskExternalId.output.someKey}"
    )
    method: Literal["POST", "GET", "PUT"] | str = Field(
        description="The HTTP method of the request. It can also be a reference like ${myTaskExternalId.output.someKey}"
    )
    query_parameters: dict[str, JsonValue] | str | None = Field(
        None,
        description="The query parameters of the request. It can also be a reference like ${myTaskExternalId.output.someKey}",
    )
    body: JsonValue | str | None = Field(
        None, description="The body of the request. It can also be a reference like ${myTaskExternalId.output.someKey}"
    )
    request_timeout_in_millis: float | str | None = Field(
        None,
        description="The timeout for the request in milliseconds. It can also be a reference like ${myTaskExternalId.output.someKey}",
    )
    cdf_version_header: Literal["alpha", "beta"] | str | None = Field(
        None, description="The Cognite Data Fusion version header to use for the request."
    )


class CDFTaskParameters(BaseModelResource):
    cdf_request: CDFRequest


class DynamicRef(BaseModelResource):
    tasks: str = Field(
        description="A Reference is an expression that allows dynamically injecting input to a task during execution. References can be used to reference the input of the Workflow, the output of a previous task in the Workflow, or the input of a previous task in the Workflow. Note that the injected value must be valid in the context of the property it is injected into. Example Task reference: ${myTaskExternalId.output.someKey} Example Workflow input reference: ${workflow.input.myKey}"
    )


class DynamicTaskParameters(BaseModelResource):
    dynamic: DynamicRef = Field(description="Reference to another task to use as the definition for this task.")


class SubworkflowInlineTasks(BaseModelResource):
    """Inline definition of tasks for a subworkflow."""

    tasks: "list[Task]" = Field(description="Inline definition of tasks for the subworkflow.")


# SubworkflowRef can be either a reference to an existing workflow version OR an inline definition of tasks
SubworkflowRef = Annotated[
    WorkflowVersionId | SubworkflowInlineTasks,
    Field(
        description="Reference to the subworkflow to execute. This can be either a reference to an existing workflow version (with workflowExternalId and version) or an inline definition of tasks."
    ),
]


class SubworkflowTaskParameters(BaseModelResource):
    subworkflow: SubworkflowRef = Field(description="Reference to the subworkflow to execute.")


class SimulatorInputUnit(BaseModelResource):
    name: str = Field(description="Name of the unit.")


class SimulatorInput(BaseModelResource):
    reference_id: str = Field(description="Reference id of the value to override.")
    value: str | int | float | list[str] | list[int] | list[float] = Field(
        description="Override the value used for a simulation run."
    )
    unit: SimulatorInputUnit | None = Field(None, description="Override the unit of the value")


class SimulationRef(BaseModelResource):
    routine_external_id: str = Field(description="The external id of the routine to be executed.")
    run_time: int | None = Field(
        None,
        description="Run time in milliseconds. Reference timestamp used for data pre-processing and data sampling.",
    )
    inputs: list[SimulatorInput] | None = Field(
        None, description="List of inputs to be provided to the simulation.", max_length=200
    )


class SimulationTaskParameters(BaseModelResource):
    simulation: SimulationRef = Field(description="Reference to the simulation to execute.")


class TaskId(BaseModelResource):
    external_id: str = Field(
        max_length=255, description="The external ID provided by the client. Must be unique for the resource type."
    )


class TaskDefinition(BaseModelResource, ABC):
    external_id: str = Field(
        max_length=255,
        description="Identifier for the task. Must be unique within the version. No trailing or leading whitespace and no null characters allowed.",
    )
    type: str
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


class FunctionTask(TaskDefinition):
    type: Literal["function"] = "function"
    parameters: FunctionTaskParameters


class TransformationTask(TaskDefinition):
    type: Literal["transformation"] = "transformation"
    parameters: TransformationTaskParameters


class CDFTask(TaskDefinition):
    type: Literal["cdfRequest"] = "cdfRequest"
    parameters: CDFTaskParameters


class DynamicTask(TaskDefinition):
    type: Literal["dynamic"] = "dynamic"
    parameters: DynamicTaskParameters


class SubworkflowTask(TaskDefinition):
    type: Literal["subworkflow"] = "subworkflow"
    parameters: SubworkflowTaskParameters


class SimulationTask(TaskDefinition):
    type: Literal["simulation"] = "simulation"
    parameters: SimulationTaskParameters


Task = Annotated[
    FunctionTask | TransformationTask | CDFTask | DynamicTask | SubworkflowTask | SimulationTask,
    Field(discriminator="type"),
]


class WorkflowDefinition(BaseModelResource):
    description: str | None = Field(
        default=None,
        max_length=500,
        description="The description of the workflow version.",
    )
    tasks: list[Task]


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
