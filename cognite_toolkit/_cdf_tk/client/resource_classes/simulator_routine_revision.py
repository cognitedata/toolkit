from typing import Any

from cognite_toolkit._cdf_tk.client._resource_base import RequestResource, ResponseResource

from .identifiers import ExternalId


class SimulatorRoutineRevisionRequest(RequestResource):
    """Request class for creating a simulator routine revision."""

    external_id: str
    routine_external_id: str
    configuration: dict[str, Any]
    script: list[dict[str, Any]] | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class SimulatorRoutineRevisionResponse(ResponseResource[SimulatorRoutineRevisionRequest]):
    """Response class for a simulator routine revision."""

    id: int
    external_id: str
    routine_external_id: str
    simulator_external_id: str
    model_external_id: str
    simulator_integration_external_id: str
    configuration: dict[str, Any]
    script: list[dict[str, Any]] | None = None
    created_by_user_id: str | None = None
    version_number: int
    data_set_id: int | None = None
    created_time: int
    last_updated_time: int

    def as_request_resource(self) -> SimulatorRoutineRevisionRequest:
        return SimulatorRoutineRevisionRequest(
            external_id=self.external_id,
            routine_external_id=self.routine_external_id,
            configuration=self.configuration,
            script=self.script,
        )
