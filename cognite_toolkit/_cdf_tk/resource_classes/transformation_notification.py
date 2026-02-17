from pydantic import Field

from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import TransformationNotificationId

from .base import ToolkitResource


class TransformationNotificationYAML(ToolkitResource):
    destination: str = Field(description="Email address where notifications should be sent.")
    transformation_external_id: str = Field(description="Transformation external ID to subscribe.")

    def as_id(self) -> TransformationNotificationId:
        return TransformationNotificationId(
            transformation_external_id=self.transformation_external_id, destination=self.destination
        )
