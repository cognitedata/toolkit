from typing import Any

from pydantic import Field

from .base import BaseModelResource, ToolkitResource
from .location import LocationFilterViewId


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

    Fields:
    - space: The space of the InField location configuration
    - external_id: The external ID of the InField location configuration
    - root_location_external_id: Reference to the LocationFilter external ID (same as external_id)
    - app_instance_space: Application instance space
    - feature_toggles: Feature toggles configuration
    - name: The name of the location filter (used for LocationFilter creation)
    - description: The description of the location filter (used for LocationFilter creation)
    - instance_spaces: The list of spaces that instances are in (used for LocationFilter creation)
    - views: The views associated with the location (used for LocationFilter creation)
    """

    space: str
    external_id: str

    root_location_external_id: str | None = None
    feature_toggles: FeatureToggles | None = None
    app_instance_space: str | None = None
    name: str | None = Field(
        default=None, description="The name of the location filter (used for LocationFilter creation)"
    )
    description: str | None = Field(
        default=None, description="The description of the location filter (used for LocationFilter creation)"
    )
    instance_spaces: list[str] | None = Field(
        default=None, description="The list of spaces that instances are in (used for LocationFilter creation)"
    )
    views: list[LocationFilterViewId] | None = Field(
        default=None, description="The views associated with the location (used for LocationFilter creation)"
    )


class DataFilterPath(BaseModelResource):
    """Path reference for data filters."""

    space: str
    external_id: str


class DataFilter(BaseModelResource):
    """Data filter configuration for a resource type."""

    path: DataFilterPath | None = None
    instance_spaces: list[str] | None = None


class DataFilters(BaseModelResource):
    """Data filters configuration."""

    files: DataFilter | None = None
    assets: DataFilter | None = None
    operations: DataFilter | None = None
    time_series: DataFilter | None = None
    notifications: DataFilter | None = None
    maintenance_orders: DataFilter | None = None


class DataStorage(BaseModelResource):
    """Data storage configuration."""

    root_location: DataFilterPath | None = None
    app_instance_space: str | None = None


class ViewMapping(BaseModelResource):
    """View mapping configuration."""

    space: str
    version: str
    external_id: str


class ViewMappings(BaseModelResource):
    """View mappings configuration."""

    asset: ViewMapping | None = None
    operation: ViewMapping | None = None
    notification: ViewMapping | None = None
    maintenance_order: ViewMapping | None = None


class Discipline(BaseModelResource):
    """Discipline configuration."""

    name: str
    external_id: str


class InFieldCDMLocationConfigYAML(ToolkitResource):
    """Properties for InFieldCDMLocationConfig node.

    Fields:
    - space: The space of the InField CDM location configuration
    - external_id: The external ID of the InField CDM location configuration
    - name: The name of the location configuration
    - description: The description of the location configuration
    - feature_toggles: Feature toggles configuration
    - app_instance_space: Application instance space
    - access_management: Access management configuration
    - data_filters: Data filters configuration for various resource types
    - data_storage: Data storage configuration
    - view_mappings: View mappings configuration
    - disciplines: List of disciplines
    - data_exploration_config: Data exploration configuration
    """

    space: str
    external_id: str

    name: str | None = None
    description: str | None = None
    feature_toggles: FeatureToggles | None = None
    app_instance_space: str | None = None
    access_management: AccessManagement | None = None
    data_filters: DataFilters | None = None
    data_storage: DataStorage | None = None
    view_mappings: ViewMappings | None = None
    disciplines: list[Discipline] | None = None
    data_exploration_config: dict[str, Any] | None = None
