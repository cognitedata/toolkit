from typing import Any, Literal, TypeAlias

from pydantic import Field, model_serializer

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, RequestResource, ResponseResource

from .identifiers import ExternalId


class Disabled(BaseModelObject):
    enabled: Literal[False] = False

    @model_serializer()
    def serialize(self) -> dict[str, Any]:
        # Always serialize as {"enabled": False}, even if model_dump(exclude_unset=True)
        return {"enabled": False}


class ScheduleConfig(BaseModelObject):
    """Schedule configuration for the routine."""

    enabled: Literal[True] = True
    cron_expression: str


class DataSamplingConfig(BaseModelObject):
    """Data sampling configuration for the routine."""

    enabled: Literal[True] = True
    validation_window: int | None = None
    sampling_window: int
    granularity: int


Aggregate = Literal["average", "interpolation", "stepInterpolation"]
LogicalCheckOperator = Literal["eq", "ne", "gt", "ge", "lt", "le"]


class LogicalCheckConfig(BaseModelObject):
    """Logical check configuration for validating time series data."""

    enabled: bool = True
    timeseries_external_id: str
    aggregate: Aggregate
    operator: LogicalCheckOperator
    value: float


class SteadyStateDetectionConfig(BaseModelObject):
    enabled: bool = True
    timeseries_external_id: str | None = None
    aggregate: Aggregate
    min_section_size: int
    var_threshold: float
    slope_threshold: float


ValueType: TypeAlias = Literal["STRING", "DOUBLE", "STRING_ARRAY", "DOUBLE_ARRAY"]


class Unit(BaseModelObject):
    """Unit definition for inputs and outputs."""

    name: str
    quantity: str


class RoutineInputConstantConfig(BaseModelObject):
    """Input configuration for the routine."""

    name: str
    reference_id: str
    value: str | float | list[str] | list[float]
    value_type: ValueType = "STRING"
    unit: Unit | None = None
    save_timeseries_external_id: str | None = None


class RoutineInputTimeseriesConfig(BaseModelObject):
    """Input configuration for the routine."""

    name: str
    reference_id: str
    source_external_id: str
    aggregate: Aggregate
    unit: Unit | None = None
    save_timeseries_external_id: str | None = None


class RoutineOutputConfig(BaseModelObject):
    """Output configuration for the routine."""

    name: str
    reference_id: str
    unit: Unit | None = None
    value_type: ValueType = "STRING"
    save_timeseries_external_id: str | None = None


# ========== Main Configuration ==========
class SimulatorRoutineConfiguration(BaseModelObject):
    """Complete configuration for a simulator routine revision."""

    schedule: ScheduleConfig | Disabled
    data_sampling: DataSamplingConfig | Disabled
    logical_check: list[LogicalCheckConfig] = Field(default_factory=list)
    steady_state_detection: list[SteadyStateDetectionConfig] = Field(default_factory=list)
    inputs: list[RoutineInputConstantConfig] | None = None
    outputs: list[RoutineOutputConfig] | None = None


class ScriptStep(BaseModelObject):
    """A Get or Set step within a script stage."""

    order: int
    description: str | None = None
    step_type: Literal["Get", "Set", "Command"]
    arguments: dict[str, Any]


class ScriptStage(BaseModelObject):
    """A stage in the script containing multiple steps."""

    order: int
    description: str | None = None
    steps: list[ScriptStep]


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
