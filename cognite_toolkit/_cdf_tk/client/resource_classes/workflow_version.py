from typing import Annotated, Any, Literal

from pydantic import Field, JsonValue, field_validator
from pydantic_core.core_schema import ValidationInfo

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    RequestResource,
    ResponseResource,
)

from .identifiers import Identifier, WorkflowVersionId


class TaskId(Identifier):
    external_id: str

    def __str__(self) -> str:
        return f"externalId='{self.external_id}'"


class TaskParameterDefinition(BaseModelObject):
    type: str


class CogniteFunctionRef(BaseModelObject):
    external_id: str
    data: JsonValue | str | None = None


class FunctionTaskParameters(TaskParameterDefinition):
    type: Literal["function"] = Field("function", exclude=True)
    function: CogniteFunctionRef
    is_async_complete: bool | None = None


class TransformationRef(BaseModelObject):
    external_id: str
    concurrency_policy: Literal["fail", "waitForCurrent", "restartAfterCurrent"] | None = None
    use_transformation_credentials: bool | None = None


class TransformationTaskParameters(TaskParameterDefinition):
    type: Literal["transformation"] = Field("transformation", exclude=True)
    transformation: TransformationRef


class CDFRequest(BaseModelObject):
    resource_path: str
    method: str
    query_parameters: JsonValue | str | None = None
    body: JsonValue | str | None = None
    request_timeout_in_millis: float | str | None = None
    cdf_version_header: str | None = None


class CDFTaskParameters(TaskParameterDefinition):
    type: Literal["cdf"] = Field("cdf", exclude=True)
    cdf_request: CDFRequest


class DynamicRef(BaseModelObject):
    tasks: str


class DynamicTaskParameters(TaskParameterDefinition):
    type: Literal["dynamic"] = Field("dynamic", exclude=True)
    dynamic: DynamicRef


class SubworkflowRef(BaseModelObject):
    tasks: "list[Task]"


class SubworkflowTaskParameters(TaskParameterDefinition):
    type: Literal["subworkflow"] = Field("subworkflow", exclude=True)
    subworkflow: WorkflowVersionId | SubworkflowRef


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


class SimulationTaskParameters(TaskParameterDefinition):
    type: Literal["simulation"] = Field("simulation", exclude=True)
    simulation: SimulationRef


Parameter = Annotated[
    FunctionTaskParameters
    | TransformationTaskParameters
    | CDFTaskParameters
    | DynamicTaskParameters
    | SubworkflowTaskParameters
    | SimulationTaskParameters,
    Field(discriminator="type"),
]


class Task(BaseModelObject):
    external_id: str
    type: str
    name: str | None = None
    description: str | None = None
    retries: int | None = None
    timeout: int | None = None
    on_failure: Literal["abortWorkflow", "skipTask"] = "abortWorkflow"
    depends_on: list[TaskId] | None = None
    parameters: Parameter | None = None

    @field_validator("parameters", mode="before")
    @classmethod
    def move_type_to_field(cls, value: Any, info: ValidationInfo) -> Any:
        if not isinstance(value, dict) or "type" not in info.data:
            return value
        value = dict(value)
        value["type"] = info.data["type"]
        return value


class WorkflowDefinition(BaseModelObject):
    description: str | None = None
    tasks: list[Task]


class WorkflowVersion(BaseModelObject):
    workflow_external_id: str
    version: str
    workflow_definition: WorkflowDefinition

    def as_id(self) -> WorkflowVersionId:
        return WorkflowVersionId(workflow_external_id=self.workflow_external_id, version=self.version)


class WorkflowVersionRequest(WorkflowVersion, RequestResource): ...


class WorkflowVersionResponse(WorkflowVersion, ResponseResource[WorkflowVersionRequest]):
    created_time: int
    last_updated_time: int

    def as_request_resource(self) -> WorkflowVersionRequest:
        return WorkflowVersionRequest.model_validate(self.dump(), extra="ignore")
