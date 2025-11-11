from typing import Any

from .base import BaseModelResource, ToolkitResource


class ObservationFeatureToggles(BaseModelResource):
    """Feature toggles for observations."""

    is_enabled: bool | None = None
    is_write_back_enabled: bool | None = None
    notifications_endpoint_external_id: str | None = None
    attachments_endpoint_external_id: str | None = None


class FeatureToggles(BaseModelResource):
    """Feature toggles for InField location configuration."""

    three_d: bool | None = None
    trends: bool | None = None
    documents: bool | None = None
    workorders: bool | None = None
    notifications: bool | None = None
    media: bool | None = None
    template_checklist_flow: bool | None = None
    workorder_checklist_flow: bool | None = None
    observations: ObservationFeatureToggles | None = None


class AccessManagement(BaseModelResource):
    """Access management configuration."""

    template_admins: list[str] | None = None  # list of CDF group external IDs
    checklist_admins: list[str] | None = None  # list of CDF group external IDs


class ResourceFilters(BaseModelResource):
    """Resource filters."""

    spaces: list[str] | None = None


class RootLocationDataFilters(BaseModelResource):
    """Data filters for root location."""

    general: ResourceFilters | None = None
    assets: ResourceFilters | None = None
    files: ResourceFilters | None = None
    timeseries: ResourceFilters | None = None


class DataExplorationConfig(BaseModelResource):
    """Properties for DataExplorationConfig node.

    Contains configuration for data exploration features:
    - observations: Observations feature configuration
    - activities: Activities configuration
    - documents: Document configuration
    - notifications: Notifications configuration
    - assets: Asset page configuration
    """

    space: str | None = None
    external_id: str | None = None

    observations: dict[str, Any] | None = None  # ObservationsConfigFeature
    activities: dict[str, Any] | None = None  # ActivitiesConfiguration
    documents: dict[str, Any] | None = None  # DocumentConfiguration
    notifications: dict[str, Any] | None = None  # NotificationsConfiguration
    assets: dict[str, Any] | None = None  # AssetPageConfiguration


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

    space: str
    external_id: str

    root_location_external_id: str | None = None
    feature_toggles: FeatureToggles | None = None
    app_instance_space: str | None = None
    access_management: AccessManagement | None = None
    data_filters: RootLocationDataFilters | None = None
    data_exploration_config: DataExplorationConfig | None = None
