from typing import ClassVar

from pydantic import Field

from cognite_toolkit._cdf_tk.client.data_classes.search_config import SearchConfigWrite

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
    _cdf_resource: ClassVar[type[SearchConfigWrite]] = SearchConfigWrite

    view: ViewId
    use_as_name: str | None = None
    use_as_description: str | None = None
    columns_layout: list[PropertyConfig] | None = None
    filter_layout: list[PropertyConfig] | None = None
    properties_layout: list[PropertyConfig] | None = None
