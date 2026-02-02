from typing import Any, Literal

from pydantic import Field

from cognite_toolkit._cdf_tk.client._resource_base import ResponseResource, UpdatableRequestResource

from .identifiers import ExternalId


class SimulatorRoutineRequest(UpdatableRequestResource):
    """Request class for creating or updating a simulator routine."""

    # The 'id' field is not part of the request when creating a new resource,
    # but is needed when updating an existing resource.
    id: int | None = Field(default=None, exclude=True)
    external_id: str
    model_external_id: str
    simulator_integration_external_id: str
    name: str
    description: str | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)

    def as_update(self, mode: Literal["patch", "replace"]) -> dict[str, Any]:
        if self.id is None:
            raise ValueError("id must be provided to create an update dictionary")
        return {
            "id": self.id,
            "update": {
                "name": {"set": self.name},
                **{"description": {"set": self.description} if self.description is not None else {}},
            },
        }


class SimulatorRoutineResponse(ResponseResource[SimulatorRoutineRequest]):
    """Response class for a simulator routine."""

    id: int
    external_id: str
    simulator_external_id: str
    model_external_id: str
    simulator_integration_external_id: str
    name: str
    description: str | None = None
    data_set_id: int | None = None
    created_time: int
    last_updated_time: int

    def as_request_resource(self) -> SimulatorRoutineRequest:
        return SimulatorRoutineRequest.model_validate(self.dump(), extra="ignore")
