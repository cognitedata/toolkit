from typing import Any, Literal

from cognite_toolkit._cdf_tk.client.data_classes.base import RequestUpdateable, ResponseResource

from .identifiers import ExternalId


class SimulatorModelRequest(RequestUpdateable):
    external_id: str
    simulator_external_id: str
    name: str
    description: str | None = None
    data_set_id: int
    type: str

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)

    def as_update(self, mode: Literal["patch", "replace"], id: int | None = None) -> dict[str, Any]:
        if id is None:
            raise ValueError("id must be provided to create an update dictionary")
        return {
            "id": id,
            "update": {
                "name": {"set": self.name},
                **{"description": {"set": self.description} if self.description is not None else {}},
            },
        }


class SimulatorModelResponse(ResponseResource[SimulatorModelRequest]):
    id: int
    external_id: str
    simulator_external_id: str
    name: str
    description: str | None = None
    data_set_id: int
    type: str | None = None
    created_time: int
    last_updated_time: int

    def as_request_resource(self) -> SimulatorModelRequest:
        return SimulatorModelRequest.model_validate(self.dump(), extra="ignore")
