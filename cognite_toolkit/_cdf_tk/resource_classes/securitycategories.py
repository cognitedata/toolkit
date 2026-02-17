from pydantic import Field

from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import NameId

from .base import ToolkitResource


class SecurityCategoriesYAML(ToolkitResource):
    name: str = Field(description="The name of the data set.")

    def as_id(self) -> NameId:
        return NameId(name=self.name)
