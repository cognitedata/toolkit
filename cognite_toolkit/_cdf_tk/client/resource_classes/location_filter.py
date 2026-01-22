from typing import Literal

from pydantic import Field

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, RequestResource, ResponseResource

from .data_modeling import DataModelReference
from .identifiers import ExternalId, InternalId


class LocationFilterScene(BaseModelObject):
    """Scene configuration for a location filter."""

    external_id: str
    space: str


class LocationFilterView(BaseModelObject):
    """View identifier with entity type for location filter."""

    external_id: str
    space: str
    version: str
    represents_entity: Literal["MAINTENANCE_ORDER", "OPERATION", "NOTIFICATION", "ASSET"] | None = None


class AssetCentricSubFilter(BaseModelObject):
    """Sub-filter for asset-centric resource types."""

    data_set_ids: list[int] | None = None
    asset_subtree_ids: list[ExternalId | InternalId] | None = None
    external_id_prefix: str | None = None


class AssetCentricFilter(BaseModelObject):
    """Filter definition for asset-centric resource types."""

    assets: AssetCentricSubFilter | None = None
    events: AssetCentricSubFilter | None = None
    files: AssetCentricSubFilter | None = None
    timeseries: AssetCentricSubFilter | None = None
    sequences: AssetCentricSubFilter | None = None
    data_set_ids: list[int] | None = None
    asset_subtree_ids: list[ExternalId | InternalId] | None = None
    external_id_prefix: str | None = None


class LocationFilter(BaseModelObject):
    """Base class for location filter with common fields."""

    external_id: str
    name: str
    description: str | None = None
    parent_id: int | None = None
    data_models: list[DataModelReference] | None = None
    instance_spaces: list[str] | None = None
    scene: LocationFilterScene | None = None
    asset_centric: AssetCentricFilter | None = None
    views: list[LocationFilterView] | None = None
    data_modeling_type: Literal["HYBRID", "DATA_MODELING_ONLY"] | None = None


class LocationFilterRequest(LocationFilter, RequestResource):
    """Request resource for creating/updating location filters."""

    # This is not part of the request payload, but we need it to identify existing resources for updates.
    id: int | None = Field(default=None, exclude=True)

    def as_id(self) -> InternalId:
        if self.id is None:
            raise ValueError("Cannot get ID for LocationFilterRequest without 'id' set.")
        return InternalId(id=self.id)


class LocationFilterResponse(LocationFilter, ResponseResource[LocationFilterRequest]):
    """Response resource for location filters."""

    id: int
    created_time: int
    updated_time: int
    locations: list["LocationFilterResponse"] | None = None

    def as_request_resource(self) -> LocationFilterRequest:
        return LocationFilterRequest.model_validate(self.dump(), extra="ignore")
