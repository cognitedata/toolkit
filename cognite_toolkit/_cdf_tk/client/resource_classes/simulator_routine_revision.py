from typing import Any, Literal

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, RequestResource, ResponseResource

from .identifiers import ExternalId


# ========== Schedule Configuration ==========
class ScheduleConfig(BaseModelObject):
    """Schedule configuration for the routine."""

    enabled: bool = False


# ========== Data Sampling Configuration ==========
class DataSamplingConfig(BaseModelObject):
    """Data sampling configuration for the routine."""

    enabled: bool = False


# ========== Logical Check Configuration ==========
LogicalCheckAggregate = Literal["average", "min", "max", "sum", "count"]
LogicalCheckOperator = Literal["eq", "ne", "gt", "ge", "lt", "le"]


class LogicalCheckConfig(BaseModelObject):
    """Logical check configuration for validating time series data."""

    enabled: bool = True
    timeseries_external_id: str
    aggregate: LogicalCheckAggregate
    operator: LogicalCheckOperator
    value: float


# ========== Steady State Detection Configuration ==========
SteadyStateAggregate = Literal["average", "min", "max", "sum", "count"]


class SteadyStateDetectionConfig(BaseModelObject):
    """Steady state detection configuration for time series data."""

    enabled: bool = True
    timeseries_external_id: str
    aggregate: SteadyStateAggregate
    min_section_size: int
    var_threshold: float
    slope_threshold: float


# ========== Input/Output Configuration ==========
class RoutineInputConfig(BaseModelObject):
    """Input configuration for the routine."""

    name: str
    reference_id: str
    value: Any | None = None
    value_type: str | None = None
    unit: str | None = None
    unit_type: str | None = None
    source_external_id: str | None = None
    source_type: str | None = None
    aggregate: str | None = None
    save_timeseries_external_id: str | None = None


class RoutineOutputConfig(BaseModelObject):
    """Output configuration for the routine."""

    name: str
    reference_id: str
    value_type: str | None = None
    unit: str | None = None
    unit_type: str | None = None
    save_timeseries_external_id: str | None = None


# ========== Main Configuration ==========
class SimulatorRoutineConfiguration(BaseModelObject):
    """Complete configuration for a simulator routine revision."""

    schedule: ScheduleConfig | None = None
    data_sampling: DataSamplingConfig | None = None
    logical_check: list[LogicalCheckConfig] | None = None
    steady_state_detection: list[SteadyStateDetectionConfig] | None = None
    inputs: list[RoutineInputConfig] | None = None
    outputs: list[RoutineOutputConfig] | None = None


# ========== Script Configuration ==========
ScriptStepType = Literal["Get", "Set", "Command"]


class ScriptStepArguments(BaseModelObject):
    """Arguments for a script step."""

    reference_id: str | None = None


class ScriptStep(BaseModelObject):
    """A single step within a script stage."""

    order: int
    description: str | None = None
    step_type: ScriptStepType
    arguments: ScriptStepArguments | dict[str, Any]


class ScriptStage(BaseModelObject):
    """A stage in the script containing multiple steps."""

    order: int
    description: str | None = None
    steps: list[ScriptStep]


# ========== Main Resource Classes ==========
class SimulatorRoutineRevision(BaseModelObject):
    external_id: str
    routine_external_id: str
    configuration: SimulatorRoutineConfiguration
    script: list[ScriptStage] | None = None


class SimulatorRoutineRevisionRequest(RequestResource, SimulatorRoutineRevision):
    """Request class for creating a simulator routine revision."""

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class SimulatorRoutineRevisionResponse(ResponseResource[SimulatorRoutineRevisionRequest], SimulatorRoutineRevision):
    """Response class for a simulator routine revision."""

    id: int
    simulator_external_id: str
    model_external_id: str
    simulator_integration_external_id: str
    created_by_user_id: str
    version_number: int
    data_set_id: int
    created_time: int

    def as_request_resource(self) -> SimulatorRoutineRevisionRequest:
        return SimulatorRoutineRevisionRequest.model_validate(self.dump(), extra="ignore")
