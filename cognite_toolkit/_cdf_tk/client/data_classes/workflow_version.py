from typing import Literal

from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client.data_classes.base import (
    BaseModelObject,
    Identifier,
    RequestResource,
    ResponseResource,
)


class WorkflowVersionId(Identifier):
    workflow_external_id: str
    version: str

    def __str__(self) -> str:
        return f"workflowExternalId='{self.workflow_external_id}', version='{self.version}'"


class TaskId(BaseModelObject):
    external_id: str


class CogniteFunctionRef(BaseModelObject):
    external_id: str
    data: JsonValue | None = None


class FunctionTaskParameters(BaseModelObject):
    function: CogniteFunctionRef
    is_async_complete: bool | None = None


class TransformationRef(BaseModelObject):
    external_id: str
    concurrency_policy: Literal["fail", "waitForCurrent", "restartAfterCurrent"] | None = None
    use_transformation_credentials: bool | None = None


class TransformationTaskParameters(BaseModelObject):
    transformation: TransformationRef


class CDFRequest(BaseModelObject):
    resource_path: str
    method: str
    query_parameters: JsonValue | None = None
    body: JsonValue | None = None
    request_timeout_in_millis: float | str | None = None
    cdf_version_header: str | None = None


class CDFTaskParameters(BaseModelObject):
    cdf_request: CDFRequest


class DynamicRef(BaseModelObject):
    tasks: str


class DynamicTaskParameters(BaseModelObject):
    dynamic: DynamicRef


class SubworkflowRef(BaseModelObject):
    workflow_external_id: str | None = None
    version: str | None = None
    tasks: "list[Task] | None" = None


class SubworkflowTaskParameters(BaseModelObject):
    subworkflow: SubworkflowRef


class SimulatorInputUnit(BaseModelObject):
    name: str


class SimulatorInput(BaseModelObject):
    reference_id: str
    value: str | int | float | list[str] | list[int] | list[float]
    unit: SimulatorInputUnit | None = None


class SimulationRef(BaseModelObject):
    routine_external_id: str
    run_time: int | None = None
    inputs: list[SimulatorInput] | None = None


class SimulationTaskParameters(BaseModelObject):
    simulation: SimulationRef


class Task(BaseModelObject):
    external_id: str
    type: str
    name: str | None = None
    description: str | None = None
    retries: int | None = None
    timeout: int | None = None
    on_failure: Literal["abortWorkflow", "skipTask"] | None = None
    depends_on: list[TaskId] | None = None
    parameters: (
        FunctionTaskParameters
        | TransformationTaskParameters
        | CDFTaskParameters
        | DynamicTaskParameters
        | SubworkflowTaskParameters
        | SimulationTaskParameters
        | None
    ) = None


class WorkflowDefinition(BaseModelObject):
    description: str | None = None
    tasks: list[Task] | None = None


class WorkflowVersion(BaseModelObject):
    workflow_external_id: str
    version: str
    workflow_definition: WorkflowDefinition | None = None

    def as_id(self) -> WorkflowVersionId:
        return WorkflowVersionId(workflow_external_id=self.workflow_external_id, version=self.version)


class WorkflowVersionRequest(WorkflowVersion, RequestResource): ...


class WorkflowVersionResponse(WorkflowVersion, ResponseResource[WorkflowVersionRequest]):
    created_time: int
    last_updated_time: int

    def as_request_resource(self) -> WorkflowVersionRequest:
        return WorkflowVersionRequest.model_validate(self.dump(), extra="ignore")
