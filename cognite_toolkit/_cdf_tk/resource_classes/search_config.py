from pydantic import Field

from .base import BaseModelResource, ToolkitResource


class ViewId(BaseModelResource):
    space: str
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
    column_layout: list[PropertyConfig] | None = None
    filter_layout: list[PropertyConfig] | None = None
    properties_layout: list[PropertyConfig] | None = None
