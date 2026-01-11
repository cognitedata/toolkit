from typing import Literal

from cognite_toolkit._cdf_tk.client.data_classes.base import BaseModelObject, RequestResource, ResponseResource

from .identifiers import ExternalId


class LocationFilterScene(BaseModelObject):
    """Scene configuration for a location filter."""

    external_id: str
    space: str


class LocationFilterDataModel(BaseModelObject):
    """Data model identifier for a location filter."""

    external_id: str
    space: str
    version: str


class LocationFilterView(BaseModelObject):
    """View identifier with entity type for location filter."""

    external_id: str
    space: str
    version: str
    represents_entity: Literal["MAINTENANCE_ORDER", "OPERATION", "NOTIFICATION", "ASSET"] | None = None


class AssetCentricSubFilter(BaseModelObject):
    """Sub-filter for asset-centric resource types."""

    data_set_ids: list[int] | None = None
    asset_subtree_ids: list[dict[Literal["externalId", "id"], int | str]] | None = None
    external_id_prefix: str | None = None


class AssetCentricFilter(BaseModelObject):
    """Filter definition for asset-centric resource types."""

    assets: AssetCentricSubFilter | None = None
    events: AssetCentricSubFilter | None = None
    files: AssetCentricSubFilter | None = None
    timeseries: AssetCentricSubFilter | None = None
    sequences: AssetCentricSubFilter | None = None
    data_set_ids: list[int] | None = None
    asset_subtree_ids: list[dict[Literal["externalId", "id"], int | str]] | None = None
    external_id_prefix: str | None = None


class LocationFilterBase(BaseModelObject):
    """Base class for location filter with common fields."""

    external_id: str
    name: str
    description: str | None = None
    parent_id: int | None = None
    data_models: list[LocationFilterDataModel] | None = None
    instance_spaces: list[str] | None = None
    scene: LocationFilterScene | None = None
    asset_centric: AssetCentricFilter | None = None
    views: list[LocationFilterView] | None = None
    data_modeling_type: Literal["HYBRID", "DATA_MODELING_ONLY"] | None = None


class LocationFilterRequest(LocationFilterBase, RequestResource):
    """Request resource for creating/updating location filters."""

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class LocationFilterResponse(LocationFilterBase, ResponseResource[LocationFilterRequest]):
    """Response resource for location filters."""

    id: int
    created_time: int
    updated_time: int
    locations: list["LocationFilterResponse"] | None = None

    def as_request_resource(self) -> LocationFilterRequest:
        return LocationFilterRequest.model_validate(self.dump(), extra="ignore")
