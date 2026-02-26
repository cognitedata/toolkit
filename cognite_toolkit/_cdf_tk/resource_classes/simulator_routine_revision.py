from typing import Any, Literal, TypeAlias

from pydantic import Field, field_validator
from pydantic_core.core_schema import ValidationInfo

from cognite_toolkit._cdf_tk.client.identifiers.identifiers import ExternalId
from cognite_toolkit._cdf_tk.utils import humanize_collection

from .base import BaseModelResource, ToolkitResource


class Disabled(BaseModelResource):
    enabled: Literal[False] = False


class ScheduleConfig(BaseModelResource):
    """Schedule configuration for the routine."""

    enabled: Literal[True] = True
    cron_expression: str = Field(description="Cron expression representing the schedule.")


class DataSamplingConfig(BaseModelResource):
    """Data sampling configuration for the routine."""

    enabled: Literal[True] = True
    validation_window: int | None = Field(
        default=None,
        description="Validation window of the data sampling. Represented in minutes. Used when either logical check or steady state detection is enabled.",
    )
    sampling_window: int = Field(description="Sampling window of the data sampling. Represented in minutes")
    granularity: int = Field(description="Granularity of the data sampling in minutes")


Aggregate = Literal["average", "interpolation", "stepInterpolation"]
LogicalCheckOperator = Literal["eq", "ne", "gt", "ge", "lt", "le"]


class LogicalCheckConfig(BaseModelResource):
    """Logical check configuration for validating time series data."""

    enabled: bool = Field(default=True, description="Whether this logical check is enabled.")
    timeseries_external_id: str = Field(description="External ID of the time series to check.")
    aggregate: Aggregate = Field(description="Aggregation method to apply.")
    operator: LogicalCheckOperator = Field(description="Comparison operator.")
    value: float = Field(description="Value to compare against.")


class SteadyStateDetectionConfig(BaseModelResource):
    enabled: bool = Field(default=True, description="Whether this steady state detection is enabled.")
    timeseries_external_id: str | None = Field(default=None, description="External ID of the time series to monitor.")
    aggregate: Aggregate = Field(description="Aggregation method to apply.")
    min_section_size: int = Field(description="Minimum section size for steady state detection.")
    var_threshold: float = Field(description="Variance threshold for steady state detection.")
    slope_threshold: float = Field(description="Slope threshold for steady state detection.")


ValueType: TypeAlias = Literal["STRING", "DOUBLE", "STRING_ARRAY", "DOUBLE_ARRAY"]


class Unit(BaseModelResource):
    """Unit definition for inputs and outputs."""

    name: str = Field(description="Name of the unit.", min_length=1, max_length=50)
    quantity: str = Field(description="Type of the unit.", min_length=1, max_length=50)


class RoutineInputConstantConfig(BaseModelResource):
    """Input configuration for the routine."""

    name: str = Field(description="Constant name")
    reference_id: str = Field(description="Reference ID for the input.")
    value: str | float | list[str] | list[float] = Field(description="Default value for the input.")
    value_type: ValueType = Field(default="STRING", description="Type of the value.")
    unit: Unit | None = Field(default=None, description="Unit of the input.")
    save_timeseries_external_id: str | None = Field(
        default=None, description="Time series external ID to use when saving the input sample in CDF."
    )


class RoutineInputTimeseriesConfig(BaseModelResource):
    """Input configuration for the routine."""

    name: str = Field(description="Timeseries name")
    reference_id: str = Field(description="Reference ID for the input.")
    source_external_id: str = Field(description="External id of the source time series to read from.")
    aggregate: Aggregate = Field(description="Aggregation method to apply.")
    unit: Unit | None = Field(default=None, description="Unit of the input.")
    save_timeseries_external_id: str | None = Field(
        default=None, description="Time series external ID to use when saving the input sample in CDF."
    )


class RoutineOutputConfig(BaseModelResource):
    """Output configuration for the routine."""

    name: str = Field(description="Name of the output.")
    reference_id: str = Field(description="Reference ID for the output.")
    unit: Unit | None = Field(default=None, description="Unit of the output.")
    value_type: ValueType = Field(default="STRING", description="Type of the value.")
    save_timeseries_external_id: str | None = Field(default=None, description="External ID for saving to time series.")


# ========== Main Configuration ==========
class SimulatorRoutineConfiguration(BaseModelResource):
    """Complete configuration for a simulator routine revision."""

    schedule: ScheduleConfig | Disabled = Field(description="Schedule configuration.", discriminator="enabled")
    data_sampling: DataSamplingConfig | Disabled = Field(description="Data sampling configuration.")
    logical_check: list[LogicalCheckConfig] = Field(
        description="List of logical checks.", default_factory=list, min_length=0, max_length=1
    )
    steady_state_detection: list[SteadyStateDetectionConfig] = Field(
        description="List of steady state detection configurations.", min_length=0, max_length=1, default_factory=list
    )
    inputs: list[RoutineInputConstantConfig] | None = Field(default=None, description="List of input configurations.")
    outputs: list[RoutineOutputConfig] | None = Field(default=None, description="List of output configurations.")


class ScriptStep(BaseModelResource):
    """A Get or Set step within a script stage."""

    order: int = Field(description="Order of the step within the stage.")
    description: str | None = Field(default=None, description="Description of the step.")
    step_type: Literal["Get", "Set", "Command"] = Field(description="Type of the step.")
    arguments: dict[str, Any] = Field(description="Arguments for the step.")

    @field_validator("arguments", mode="after")
    @classmethod
    def validate_arguments(cls, v: dict[str, Any], info: ValidationInfo) -> dict:
        step_type = info.data.get("step_type")
        if step_type not in ("Get", "Set"):
            return v  # No validation for other step types
        required_keys: set[str] = {"referenceId"}
        missing_keys = required_keys - set(v.keys())
        if missing_keys:
            raise ValueError(
                f"Missing required argument keys for step_type '{step_type}': {humanize_collection(missing_keys)}"
            )
        return v


class ScriptStage(BaseModelResource):
    """A stage in the script containing multiple steps."""

    order: int = Field(description="Order of the stage within the script.")
    description: str | None = Field(default=None, description="Description of the stage.")
    steps: list[ScriptStep] = Field(description="List of steps in this stage.")


class SimulatorRoutineRevisionYAML(ToolkitResource):
    """Simulator routine revision YAML resource class.

    Based on: https://api-docs.cognite.com/20230101/tag/Simulator-Routines/operation/create_simulator_routine_revision_simulators_routines_revisions_post
    """

    external_id: str = Field(description="External ID of the simulator routine revision.", min_length=1, max_length=255)
    routine_external_id: str = Field(description="External ID of the simulator routine.", min_length=1, max_length=255)
    configuration: SimulatorRoutineConfiguration = Field(description="Configuration of the simulator routine revision.")
    script: list[ScriptStage] | None = Field(default=None, description="Script stages for the routine revision.")

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)
