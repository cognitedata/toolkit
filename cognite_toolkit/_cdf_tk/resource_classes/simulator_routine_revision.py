from typing import Any, Literal

from pydantic import Field

from .base import BaseModelResource, ToolkitResource


# ========== Schedule Configuration ==========
class ScheduleConfig(BaseModelResource):
    """Schedule configuration for the routine."""

    enabled: bool = Field(default=False, description="Whether scheduling is enabled.")


# ========== Data Sampling Configuration ==========
class DataSamplingConfig(BaseModelResource):
    """Data sampling configuration for the routine."""

    enabled: bool = Field(default=False, description="Whether data sampling is enabled.")


# ========== Logical Check Configuration ==========
LogicalCheckAggregate = Literal["average", "min", "max", "sum", "count"]
LogicalCheckOperator = Literal["eq", "ne", "gt", "ge", "lt", "le"]


class LogicalCheckConfig(BaseModelResource):
    """Logical check configuration for validating time series data."""

    enabled: bool = Field(default=True, description="Whether this logical check is enabled.")
    timeseries_external_id: str = Field(description="External ID of the time series to check.")
    aggregate: LogicalCheckAggregate = Field(description="Aggregation method to apply.")
    operator: LogicalCheckOperator = Field(description="Comparison operator.")
    value: float = Field(description="Value to compare against.")


# ========== Steady State Detection Configuration ==========
SteadyStateAggregate = Literal["average", "min", "max", "sum", "count"]


class SteadyStateDetectionConfig(BaseModelResource):
    """Steady state detection configuration for time series data."""

    enabled: bool = Field(default=True, description="Whether this steady state detection is enabled.")
    timeseries_external_id: str = Field(description="External ID of the time series to monitor.")
    aggregate: SteadyStateAggregate = Field(description="Aggregation method to apply.")
    min_section_size: int = Field(description="Minimum section size for steady state detection.")
    var_threshold: float = Field(description="Variance threshold for steady state detection.")
    slope_threshold: float = Field(description="Slope threshold for steady state detection.")


# ========== Input/Output Configuration ==========
class RoutineInputConfig(BaseModelResource):
    """Input configuration for the routine."""

    name: str = Field(description="Name of the input.")
    reference_id: str = Field(description="Reference ID for the input.")
    value: Any | None = Field(default=None, description="Default value for the input.")
    value_type: str | None = Field(default=None, description="Type of the value.")
    unit: str | None = Field(default=None, description="Unit of the input.")
    unit_type: str | None = Field(default=None, description="Type of unit.")
    source_external_id: str | None = Field(default=None, description="External ID of the source.")
    source_type: str | None = Field(default=None, description="Type of the source.")
    aggregate: str | None = Field(default=None, description="Aggregation method.")
    save_timeseries_external_id: str | None = Field(default=None, description="External ID for saving to time series.")


class RoutineOutputConfig(BaseModelResource):
    """Output configuration for the routine."""

    name: str = Field(description="Name of the output.")
    reference_id: str = Field(description="Reference ID for the output.")
    value_type: str | None = Field(default=None, description="Type of the value.")
    unit: str | None = Field(default=None, description="Unit of the output.")
    unit_type: str | None = Field(default=None, description="Type of unit.")
    save_timeseries_external_id: str | None = Field(default=None, description="External ID for saving to time series.")


# ========== Main Configuration ==========
class SimulatorRoutineConfiguration(BaseModelResource):
    """Complete configuration for a simulator routine revision."""

    schedule: ScheduleConfig | None = Field(default=None, description="Schedule configuration.")
    data_sampling: DataSamplingConfig | None = Field(default=None, description="Data sampling configuration.")
    logical_check: list[LogicalCheckConfig] | None = Field(default=None, description="List of logical checks.")
    steady_state_detection: list[SteadyStateDetectionConfig] | None = Field(
        default=None, description="List of steady state detection configurations."
    )
    inputs: list[RoutineInputConfig] | None = Field(default=None, description="List of input configurations.")
    outputs: list[RoutineOutputConfig] | None = Field(default=None, description="List of output configurations.")


# ========== Script Configuration ==========
ScriptStepType = Literal["Get", "Set", "Command"]


class ScriptStepArguments(BaseModelResource):
    """Arguments for a script step."""

    reference_id: str | None = Field(default=None, description="Reference ID for the step argument.")


class ScriptStep(BaseModelResource):
    """A single step within a script stage."""

    order: int = Field(description="Order of the step within the stage.")
    description: str | None = Field(default=None, description="Description of the step.")
    step_type: ScriptStepType = Field(description="Type of the step.")
    arguments: ScriptStepArguments | dict[str, Any] = Field(description="Arguments for the step.")


class ScriptStage(BaseModelResource):
    """A stage in the script containing multiple steps."""

    order: int = Field(description="Order of the stage within the script.")
    description: str | None = Field(default=None, description="Description of the stage.")
    steps: list[ScriptStep] = Field(description="List of steps in this stage.")


# ========== Main Resource Class ==========
class SimulatorRoutineRevisionYAML(ToolkitResource):
    """Simulator routine revision YAML resource class.

    Based on: https://api-docs.cognite.com/20230101/tag/Simulator-Routines/operation/create_simulator_routine_revision_simulators_routines_revisions_post
    """

    external_id: str = Field(description="External ID of the simulator routine revision.", min_length=1, max_length=255)
    routine_external_id: str = Field(description="External ID of the simulator routine.", min_length=1, max_length=255)
    configuration: SimulatorRoutineConfiguration = Field(description="Configuration of the simulator routine revision.")
    script: list[ScriptStage] | None = Field(default=None, description="Script stages for the routine revision.")
