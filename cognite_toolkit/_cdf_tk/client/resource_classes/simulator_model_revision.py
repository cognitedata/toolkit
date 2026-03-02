from typing import Literal

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, RequestResource, ResponseResource
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId


class SimulatorModelRevision(BaseModelObject):
    external_id: str
    model_external_id: str
    description: str | None = None
    file_id: int


class SimulatorModelRevisionRequest(RequestResource, SimulatorModelRevision):
    """Request class for creating a simulator model revision."""

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class SimulatorModelRevisionResponse(ResponseResource[SimulatorModelRevisionRequest], SimulatorModelRevision):
    """Response class for a simulator model revision."""

    id: int
    description: str | None = None
    simulator_external_id: str
    created_by_user_id: str
    status: Literal["unknown", "success", "failure"] = "unknown"
    status_message: str | None = None
    version_number: int
    log_id: int
    data_set_id: int
    created_time: int
    last_updated_time: int

    @classmethod
    def request_cls(cls) -> type[SimulatorModelRevisionRequest]:
        return SimulatorModelRevisionRequest
