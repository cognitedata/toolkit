from typing import ClassVar

from cognite_toolkit._cdf_tk.client.data_classes.base import RequestUpdateable, ResponseResource

from .identifiers import ExternalId


class SimulatorModelRequest(RequestUpdateable):
    container_fields: ClassVar[frozenset[str]] = frozenset()
    non_nullable_fields: ClassVar[frozenset[str]] = frozenset()
    external_id: str
    simulator_external_id: str
    name: str
    description: str | None = None
    data_set_id: int | None = None
    type: str | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class SimulatorModelResponse(ResponseResource[SimulatorModelRequest]):
    id: int
    external_id: str
    simulator_external_id: str
    name: str
    description: str | None = None
    data_set_id: int | None = None
    type: str | None = None
    created_time: int
    last_updated_time: int

    def as_request_resource(self) -> SimulatorModelRequest:
        return SimulatorModelRequest.model_validate(self.dump(), extra="ignore")
