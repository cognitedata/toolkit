from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, RequestResource, ResponseResource

from ..identifiers._data_modeling import ViewIdNoVersionUntyped
from .data_modeling import ViewReferenceNoVersion


class SearchConfigViewProperty(BaseModelObject):
    """Configuration for a property in search configuration."""

    property: str
    disabled: bool | None = None
    selected: bool | None = None
    hidden: bool | None = None


class SearchConfigBase(BaseModelObject):
    """Base class for search configuration with common fields."""

    view: ViewIdNoVersionUntyped
    use_as_name: str | None = None
    use_as_description: str | None = None
    columns_layout: list[SearchConfigViewProperty] | None = None
    filter_layout: list[SearchConfigViewProperty] | None = None
    properties_layout: list[SearchConfigViewProperty] | None = None

    def as_id(self) -> ViewReferenceNoVersion:
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

    @classmethod
    def request_cls(cls) -> type[SearchConfigRequest]:
        return SearchConfigRequest
