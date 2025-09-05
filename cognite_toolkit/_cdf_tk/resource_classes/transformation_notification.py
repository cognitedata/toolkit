from pydantic import Field

from .base import ToolkitResource


class TransformationNotificationYAML(ToolkitResource):
    destination: str = Field(description="Email address where notifications should be sent.")
    transformation_external_id: str = Field(description="Transformation external ID to subscribe.")
