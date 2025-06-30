from pydantic import Field

from .base import ToolkitResource


class SecurityCategoriesYAML(ToolkitResource):
    name: str = Field(description="The name of the data set.")
