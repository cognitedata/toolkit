from abc import ABC
from typing import Annotated, Any, Literal

from pydantic import ConfigDict, Discriminator, JsonValue, Tag, TypeAdapter

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, RequestItem

SimulatorLogSeverity = Literal["Debug", "Information", "Warning", "Error"]


class SimulatorModelType(BaseModelObject):
    name: str
    key: str


class SimulatorUnitEntry(BaseModelObject):
    label: str
    name: str


class SimulatorQuantity(BaseModelObject):
    name: str
    label: str
    units: list[SimulatorUnitEntry]


class SimulatorResponse(BaseModelObject):
    """A simulator, such as PROSPER or DWSIM, and what its connectors support."""

    id: int
    external_id: str
    name: str
    file_extension_types: list[str]
    model_dependencies: list[dict[str, JsonValue]] | None = None
    model_types: list[SimulatorModelType] | None = None
    step_fields: list[dict[str, JsonValue]] | None = None
    unit_quantities: list[SimulatorQuantity] | None = None
    created_time: int
    last_updated_time: int


class SimulatorIntegrationResponse(BaseModelObject):
    """A connector that runs simulations for a simulator. The connector reports its status with a heartbeat."""

    id: int
    external_id: str
    simulator_external_id: str
    data_set_id: int
    heartbeat: int
    connector_version: str
    license_status: str | None = None
    simulator_version: str | None = None
    license_last_checked_time: int | None = None
    connector_status: str | None = None
    connector_status_updated_time: int | None = None
    active: bool
    log_id: int
    created_time: int
    last_updated_time: int


class SimulatorLogEntry(BaseModelObject):
    timestamp: int
    message: str
    severity: str


class SimulatorLogResponse(BaseModelObject):
    id: int
    data_set_id: int
    data: list[SimulatorLogEntry]
    severity: str | None = None
    created_time: int
    last_updated_time: int


class SimulationInputUnit(BaseModelObject):
    name: str


class SimulationInputOverride(BaseModelObject):
    """Overrides the value of a routine input for a single run."""

    reference_id: str
    value: str | float | list[str] | list[float]
    unit: SimulationInputUnit | None = None


class SimulationRunRequest(RequestItem, ABC):
    """Run a simulation. Use one of the subclasses to select what to run."""

    run_time: int | None = None
    queue: bool | None = None
    log_severity: SimulatorLogSeverity | None = None
    inputs: list[SimulationInputOverride] | None = None


class SimulationRunByRoutineRequest(SimulationRunRequest):
    """Run the latest revision of a routine."""

    routine_external_id: str

    def __str__(self) -> str:
        return f"routineExternalId='{self.routine_external_id}'"


class SimulationRunByRoutineRevisionRequest(SimulationRunRequest):
    """Run a given routine revision against a given model revision."""

    routine_revision_external_id: str
    model_revision_external_id: str

    def __str__(self) -> str:
        return (
            f"routineRevisionExternalId='{self.routine_revision_external_id}', "
            f"modelRevisionExternalId='{self.model_revision_external_id}'"
        )


def _simulation_run_request_tag(value: Any) -> str:
    if isinstance(value, dict):
        is_by_revision = "routineRevisionExternalId" in value or "routine_revision_external_id" in value
    else:
        is_by_revision = isinstance(value, SimulationRunByRoutineRevisionRequest)
    return "routine_revision" if is_by_revision else "routine"


SimulationRunRequestUnion = Annotated[
    Annotated[SimulationRunByRoutineRequest, Tag("routine")]
    | Annotated[SimulationRunByRoutineRevisionRequest, Tag("routine_revision")],
    Discriminator(_simulation_run_request_tag),
]

SimulationRunRequestAdapter: TypeAdapter[SimulationRunRequestUnion] = TypeAdapter(
    SimulationRunRequestUnion, config=ConfigDict(defer_build=True)
)


class SimulationRunResponse(BaseModelObject):
    id: int
    data_set_id: int
    simulator_external_id: str
    simulator_integration_external_id: str | None = None
    model_external_id: str
    model_revision_external_id: str
    routine_external_id: str
    routine_revision_external_id: str
    run_time: int | None = None
    simulation_time: int | None = None
    leased_until_time: int | None = None
    status: str
    status_message: str | None = None
    run_type: str
    user_id: str
    log_id: int
    created_time: int
    last_updated_time: int


class SimulationValueUnit(BaseModelObject):
    name: str
    external_id: str | None = None


class SimulationValue(BaseModelObject):
    reference_id: str
    value: JsonValue
    value_type: Literal["STRING", "DOUBLE", "STRING_ARRAY", "DOUBLE_ARRAY"] | str
    unit: SimulationValueUnit | None = None
    simulator_object_reference: dict[str, str] | None = None
    timeseries_external_id: str | None = None
    # Only set on inputs.
    overridden: bool | None = None


class SimulationRunDataResponse(BaseModelObject):
    """The input and output values of a simulation run."""

    run_id: int
    inputs: list[SimulationValue]
    outputs: list[SimulationValue]


class SimulatorModelRevisionDataResponse(BaseModelObject):
    """The flowsheets the connector extracted from a model revision file, such as unit operations and streams."""

    model_revision_external_id: str
    data_set_id: int
    flowsheets: list[dict[str, JsonValue]] | None = None
    info: dict[str, str] | None = None
    created_time: int
    last_updated_time: int
