from pydantic import Field

from .base import ToolkitResource


class ThreeDModelYAML(ToolkitResource):
    data_set_external_id: str | None = Field(
        default=None, description="The id of the dataset this 3D model belongs to."
    )
    name: str = Field(
        description="The name of the model.",
        min_length=1,
        max_length=255,
    )
    metadata: dict[str, str] | None = Field(
        default=None,
        description="Custom, application-specific metadata.",
        max_length=16,
    )
