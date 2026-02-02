from typing import Any

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, RequestResource, ResponseResource

from .identifiers import ExternalId


class SimulatorRoutineRevision(BaseModelObject):
    external_id: str
    routine_external_id: str
    configuration: dict[str, Any]
    script: list[dict[str, Any]] | None = None


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
