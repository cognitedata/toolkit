from typing import Any

from .base import BaseModelResource, ToolkitResource


class ObservationFeatureToggles(BaseModelResource):
    """Feature toggles for observations."""

    is_enabled: bool
    is_write_back_enabled: bool
    notifications_endpoint_external_id: str
    attachments_endpoint_external_id: str


class FeatureToggles(BaseModelResource):
    """Feature toggles for InField location configuration."""

    three_d: bool
    trends: bool
    documents: bool
    workorders: bool
    notifications: bool
    media: bool
    template_checklist_flow: bool
    workorder_checklist_flow: bool
    observations: ObservationFeatureToggles


class AccessManagement(BaseModelResource):
    """Access management configuration."""

    template_admins: list[str]  # list of CDF group external IDs
    checklist_admins: list[str]  # list of CDF group external IDs


class ResourceFilters(BaseModelResource):
    """Resource filters."""

    dataset_ids: list[int] | None
    asset_subtree_external_ids: list[str] | None
    root_asset_external_ids: list[str] | None
    external_id_prefix: str | None
    spaces: list[str] | None


class RootLocationDataFilters(BaseModelResource):
    """Data filters for root location."""

    general: ResourceFilters | None
    assets: ResourceFilters | None
    files: ResourceFilters | None
    timeseries: ResourceFilters | None


class DataExplorationConfig(BaseModelResource):
    """Properties for DataExplorationConfig node.

    Contains configuration for data exploration features:
    - observations: Observations feature configuration
    - activities: Activities configuration
    - documents: Document configuration
    - notifications: Notifications configuration
    - assets: Asset page configuration
    """

    external_id: str

    observations: dict[str, Any]  # ObservationsConfigFeature
    activities: dict[str, Any]  # ActivitiesConfiguration
    documents: dict[str, Any]  # DocumentConfiguration
    notifications: dict[str, Any]  # NotificationsConfiguration
    assets: dict[str, Any]  # AssetPageConfiguration


class ObservationConfig(BaseModelResource):
    external_id: str
    root_location_external_ids: list[str]
    field_configurations: dict[str, Any]


class InfieldLocationConfigYAML(ToolkitResource):
    """Properties for InFieldLocationConfig node.

    Currently migrated fields:
    - root_location_external_id: Reference to the LocationFilterDTO external ID
    - feature_toggles: Feature toggles migrated from old configuration
    - rootAsset: Direct relation to the root asset (space and externalId)
    - app_instance_space: Application instance space from appDataInstanceSpace
    - access_management: Template and checklist admin groups (from templateAdmins and checklistAdmins)
    - disciplines: List of disciplines (from disciplines in FeatureConfiguration)
    - data_filters: Data filters for general, assets, files, and timeseries (from dataFilters in old configuration)
    - data_exploration_config: Direct relation to the DataExplorationConfig node (shared across all locations)
    """

    external_id: str

    root_location_external_id: str
    feature_toggles: FeatureToggles
    classic_asset_external_id: str
    app_instance_space: str
    app_data_set: str
    access_management: AccessManagement
    data_filters: RootLocationDataFilters
    observation_config: ObservationConfig
    data_exploration_config: DataExplorationConfig
