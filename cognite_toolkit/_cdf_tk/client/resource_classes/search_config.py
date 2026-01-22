from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, RequestResource, ResponseResource

from .identifiers import Identifier


class SearchConfigViewId(Identifier):
    """Identifier for a view in search configuration."""

    space: str
    external_id: str

    def __str__(self) -> str:
        return f"space='{self.space}', externalId='{self.external_id}'"


class SearchConfigViewProperty(BaseModelObject):
    """Configuration for a property in search configuration."""

    property: str
    disabled: bool | None = None
    selected: bool | None = None
    hidden: bool | None = None


class SearchConfigBase(BaseModelObject):
    """Base class for search configuration with common fields."""

    view: SearchConfigViewId
    use_as_name: str | None = None
    use_as_description: str | None = None
    columns_layout: list[SearchConfigViewProperty] | None = None
    filter_layout: list[SearchConfigViewProperty] | None = None
    properties_layout: list[SearchConfigViewProperty] | None = None

    def as_id(self) -> SearchConfigViewId:
        return self.view


class SearchConfigRequest(SearchConfigBase, RequestResource):
    """Request resource for creating/updating search configuration."""

    # This is required when updating an existing search config
    id: int | None = None


class SearchConfigResponse(SearchConfigBase, ResponseResource[SearchConfigRequest]):
    """Response resource for search configuration."""

    id: int
    created_time: int
    last_updated_time: int

    def as_request_resource(self) -> SearchConfigRequest:
        return SearchConfigRequest.model_validate(self.dump(), extra="ignore")
