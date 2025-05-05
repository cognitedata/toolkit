from pydantic import Field

from .base import ToolkitResource


class SecuritytCategoriesYAML(ToolkitResource):
    name: str = Field(description="The name of the data set.")
