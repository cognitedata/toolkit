from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, RequestResource, ResponseResource
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId


class SimulatorRoutine(BaseModelObject):
    external_id: str
    model_external_id: str
    simulator_integration_external_id: str
    name: str
    description: str | None = None


class SimulatorRoutineRequest(RequestResource, SimulatorRoutine):
    """Request class for creating or updating a simulator routine."""

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class SimulatorRoutineResponse(ResponseResource[SimulatorRoutineRequest], SimulatorRoutine):
    """Response class for a simulator routine."""

    id: int
    simulator_external_id: str
    description: str | None = None
    data_set_id: int
    created_time: int
    last_updated_time: int

    @classmethod
    def request_cls(cls) -> type[SimulatorRoutineRequest]:
        return SimulatorRoutineRequest
