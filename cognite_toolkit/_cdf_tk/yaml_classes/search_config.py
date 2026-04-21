from pydantic import Field

from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import ViewNoVersionId
from cognite_toolkit._cdf_tk.constants import SPACE_FORMAT_PATTERN

from .base import BaseModelResource, ToolkitResource


class ViewId(BaseModelResource):
    space: str = Field(min_length=1, max_length=43, pattern=SPACE_FORMAT_PATTERN)
    external_id: str


class PropertyConfig(BaseModelResource):
    property_name: str = Field(alias="property")
    disabled: bool | None = None
    selected: bool | None = None
    hidden: bool | None = None


class SearchConfigYAML(ToolkitResource):
    view: ViewId
    use_as_name: str | None = None
    use_as_description: str | None = None
    columns_layout: list[PropertyConfig] | None = None
    filter_layout: list[PropertyConfig] | None = None
    properties_layout: list[PropertyConfig] | None = None

    def as_id(self) -> ViewNoVersionId:
        return ViewNoVersionId(space=self.view.space, external_id=self.view.external_id)
