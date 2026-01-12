from cognite_toolkit._cdf_tk.client.data_classes.base import BaseModelObject, RequestResource, ResponseResource
from cognite_toolkit._cdf_tk.client.data_classes.identifiers import ExternalId

from ._common import Transform


class RobotFrame(BaseModelObject):
    """The frames resource represents coordinate frames, which are used to describe how maps are aligned with
    respect to each other. For example, frames are used to describe the relative position of a context map
    (e.g., a 3D model of a location) and a robot's navigation map. Frames are aligned with each other through
    transforms, which consist of a translation (in meters) and rotation (quaternion).

    Args:
        name: Frame name.
        external_id: Frame external id. Must be unique for the resource type.
        transform: Transform of the parent frame to the current frame.
    """

    external_id: str
    name: str
    transform: Transform | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class RobotFrameRequest(RobotFrame, RequestResource):
    """Request resource for creating or updating a Frame."""

    pass


class RobotFrameResponse(RobotFrame, ResponseResource[RobotFrameRequest]):
    """Response resource for a Frame."""

    created_time: int
    updated_time: int

    def as_request_resource(self) -> RobotFrameRequest:
        return RobotFrameRequest.model_validate(self.dump(), extra="ignore")
