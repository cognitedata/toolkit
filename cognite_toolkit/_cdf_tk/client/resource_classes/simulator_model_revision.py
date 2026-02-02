from cognite_toolkit._cdf_tk.client._resource_base import RequestResource, ResponseResource

from .identifiers import ExternalId


class SimulatorModelRevisionRequest(RequestResource):
    """Request class for creating a simulator model revision."""

    external_id: str
    model_external_id: str
    description: str | None = None
    file_id: int

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class SimulatorModelRevisionResponse(ResponseResource[SimulatorModelRevisionRequest]):
    """Response class for a simulator model revision."""

    id: int
    external_id: str
    simulator_external_id: str
    model_external_id: str
    description: str | None = None
    file_id: int
    created_by_user_id: str | None = None
    status: str
    status_message: str | None = None
    version_number: int
    log_id: int | None = None
    data_set_id: int | None = None
    created_time: int
    last_updated_time: int

    def as_request_resource(self) -> SimulatorModelRevisionRequest:
        return SimulatorModelRevisionRequest(
            external_id=self.external_id,
            model_external_id=self.model_external_id,
            description=self.description,
            file_id=self.file_id,
        )
