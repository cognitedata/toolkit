from typing import Any

from .base import BaseModelResource, ToolkitResource


class FeatureToggles(BaseModelResource):
    """Feature toggles for InField location configuration."""

    three_d: bool | None = None
    trends: bool | None = None
    documents: bool | None = None
    workorders: bool | None = None
    notifications: bool | None = None
    media: bool | None = None
    template_checklist_flow: bool | None = None


class AccessManagement(BaseModelResource):
    """Access management configuration."""

    template_admins: list[str] | None = None  # list of CDF group external IDs
    checklist_admins: list[str] | None = None  # list of CDF group external IDs


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
    access_management: AccessManagement | None = None
    data_filters: DataFilters | None = None
    data_storage: DataStorage | None = None
    view_mappings: ViewMappings | None = None
    disciplines: list[Discipline] | None = None
    data_exploration_config: dict[str, Any] | None = None
