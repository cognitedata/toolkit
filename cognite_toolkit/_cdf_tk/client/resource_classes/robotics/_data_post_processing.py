from typing import ClassVar

from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
    UpdatableRequestResource,
)
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExternalId


class RobotDataPostProcessing(BaseModelObject):
    """DataPostprocessing define types of data processing on data captured by the robot.
    DataPostprocessing enables you to automatically process data captured by the robot.

    Args:
        name: DataPostProcessing name.
        external_id: DataPostProcessing external id. Must be unique for the resource type.
        method: DataPostProcessing method. The method is used to call the right functionality on the robot.
        input_schema: Schema that defines what inputs are needed for the data postprocessing.
            The input are values that configure the data postprocessing, e.g max and min values for a gauge.
        description: Description of DataPostProcessing. Textual description of the DataPostProcessing.
    """

    external_id: str
    name: str
    method: str
    description: str | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class RobotDataPostProcessingRequest(RobotDataPostProcessing, UpdatableRequestResource):
    """Request resource for creating or updating a DataPostProcessing."""

    non_nullable_fields: ClassVar[frozenset[str]] = frozenset({"input_schema"})
    input_schema: dict[str, JsonValue] | None = None


class RobotDataPostProcessingResponse(RobotDataPostProcessing, ResponseResource[RobotDataPostProcessingRequest]):
    """Response resource for a DataPostProcessing."""

    # The response always has input_schema
    input_schema: dict[str, JsonValue]

    def as_request_resource(self) -> RobotDataPostProcessingRequest:
        return RobotDataPostProcessingRequest.model_validate(self.dump(), extra="ignore")
