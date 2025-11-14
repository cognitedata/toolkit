from typing import Literal

from pydantic import Field

from cognite_toolkit._cdf_tk.constants import StreamTemplateName

from .base import BaseModelResource, ToolkitResource


class StreamSettings(BaseModelResource):
    """Stream settings resource class."""

    template: dict[Literal["name"], StreamTemplateName] = Field(
        description="Reference to a template which should be used to define initial settings for the stream."
    )


class StreamYAML(ToolkitResource):
    """Stream YAML resource class."""

    external_id: str = Field(
        description="The external ID of the stream.",
        min_length=1,
        max_length=100,
        pattern="^[a-z]([a-z0-9_-]{0,98}[a-z0-9])?$",
    )
    settings: StreamSettings = Field(
        description="Stream settings which should be applied to a stream.",
    )
