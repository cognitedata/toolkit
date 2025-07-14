from .base import BaseModelResource, ToolkitResource


class ViewId(BaseModelResource):
    space: str
    external_id: str


class SearchConfigYAML(ToolkitResource):
    view: ViewId
    use_as_name: str | None = None
    use_as_description: str | None = None
