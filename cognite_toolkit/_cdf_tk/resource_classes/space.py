from pydantic import Field, field_validator

from cognite_toolkit._cdf_tk.constants import FORBIDDEN_SPACES, SPACE_FORMAT_PATTERN
from cognite_toolkit._cdf_tk.utils.collection import humanize_collection

from .base import ToolkitResource


class SpaceYAML(ToolkitResource):
    space: str = Field(
        description="The Space identifier (id).",
        min_length=1,
        max_length=43,
        pattern=SPACE_FORMAT_PATTERN,
    )
    name: str | None = Field(None, description="Name of the space.", max_length=1024)
    description: str | None = Field(None, description="The description of the space.", max_length=255)

    @field_validator("space")
    @classmethod
    def check_forbidden_space_value(cls, val: str) -> str:
        """Check the space name not present in forbidden set"""
        if val in FORBIDDEN_SPACES:
            raise ValueError(f"'{val}' is a reserved space. Reserved Spaces: {humanize_collection(FORBIDDEN_SPACES)}")
        return val
