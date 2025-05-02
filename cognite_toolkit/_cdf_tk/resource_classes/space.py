from pydantic import Field

from .base import ToolkitResource


class SpaceYAML(ToolkitResource):
    space: str = Field(
        description="The Space identifier (id).",
        min_length=1,
        max_length=43,
        pattern="(?!^(space|cdf|dms|pg3|shared|system|node|edge)$)(^[a-zA-Z][a-zA-Z0-9_-]{0,41}[a-zA-Z0-9]?$)",
    )
    name: str | None = Field(None, description="Name of the space.", max_length=1024)
    description: str | None = Field(None, description="The description of the space.", max_length=255)
