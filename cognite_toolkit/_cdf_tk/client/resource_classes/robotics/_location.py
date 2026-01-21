from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, ResponseResource, UpdatableRequestResource
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExternalId


class RobotLocation(BaseModelObject):
    """The Locations resource is used to specify the physical location of a robot. Robot missions are defined
    for a specific location. In addition, the location is used to group Missions and Map resources.

    Args:
        name: Location name.
        external_id: Location external id. Must be unique for the resource type.
        description: Description of Location. Textual description of the Location.
    """

    external_id: str
    name: str
    description: str | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class RobotLocationRequest(RobotLocation, UpdatableRequestResource):
    """Request resource for creating or updating a Location."""

    ...


class RobotLocationResponse(RobotLocation, ResponseResource[RobotLocationRequest]):
    """Response resource for a Location."""

    created_time: int
    updated_time: int

    def as_request_resource(self) -> RobotLocationRequest:
        return RobotLocationRequest.model_validate(self.dump(), extra="ignore")
