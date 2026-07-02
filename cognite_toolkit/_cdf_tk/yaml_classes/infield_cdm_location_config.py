from pydantic import Field

from cognite_toolkit._cdf_tk.client.identifiers import NodeId
from cognite_toolkit._cdf_tk.constants import SPACE_FORMAT_PATTERN

from .base import BaseModelResource, ToolkitResource
from .view_field_definitions import ViewReference


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
    observations: bool | None = None
    copilot: bool | None = None


class AccessManagement(BaseModelResource):
    """Access management configuration."""

    template_admins: list[str] | None = None  # list of CDF group external IDs
    checklist_admins: list[str] | None = None  # list of CDF group external IDs


class DataFilterPath(BaseModelResource):
    """Path reference for data filters."""

    space: str = Field(min_length=1, max_length=43, pattern=SPACE_FORMAT_PATTERN)
    external_id: str


class DataFilter(BaseModelResource):
    """Data filter configuration for a resource type."""

    path: DataFilterPath | None = None
    instance_spaces: list[str] | None = None
    external_id_prefix: str | None = None


class DataFilters(BaseModelResource):
    """Data filters configuration."""

    files: DataFilter | None = None
    assets: DataFilter | None = None
    operations: DataFilter | None = None
    timeseries: DataFilter | None = None
    notifications: DataFilter | None = None
    maintenance_orders: DataFilter | None = None


class DataStorage(BaseModelResource):
    """Data storage configuration."""

    root_location: DataFilterPath | None = None
    app_instance_space: str | None = Field(None, min_length=1, max_length=43, pattern=SPACE_FORMAT_PATTERN)


class ObservationViewWriteBack(BaseModelResource):
    """Write-back configuration for an observation view."""

    notifications_endpoint_external_id: str = Field(min_length=1)
    attachments_endpoint_external_id: str | None = Field(None, min_length=1)


class ObservationViewConfig(BaseModelResource):
    """Observation view configuration."""

    view: ViewReference
    write_back: ObservationViewWriteBack | None = None


class ViewMappings(BaseModelResource):
    """View mappings configuration."""

    asset: ViewReference | None = None
    operation: ViewReference | None = None
    notification: ViewReference | None = None
    # As of 26/04-26, activity is supported,
    # but InField will likely rename it ot maintenance_order, thus we keep
    # both for now to avoid complaining to the user of either.
    maintenance_order: ViewReference | None = None
    activity: ViewReference | None = None

    file: ViewReference | None = None
    # As of 27/04-26, observation only supported for one view,
    # but we keep the list for future flexibility.
    observation: list[ObservationViewConfig] | None = Field(None, min_length=1, max_length=1)


class DataExplorationConfig(BaseModelResource):
    """Data exploration configuration."""

    asset_properties_card_view: ViewReference | None = None
    asset_activities_card_view: ViewReference | None = None
    asset_notifications_card_view: ViewReference | None = None


# Pydantic attribute name -> YAML/API key for card views used in build dependency and validation rules.
INFIELD_CDM_CARD_VIEW_ATTR_TO_JSON_KEY: dict[str, str] = {
    "asset_activities_card_view": "assetActivitiesCardView",
    "asset_notifications_card_view": "assetNotificationsCardView",
}


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

    space: str = Field(min_length=1, max_length=43, pattern=SPACE_FORMAT_PATTERN)
    external_id: str

    name: str | None = None
    description: str | None = None
    feature_toggles: FeatureToggles | None = None
    access_management: AccessManagement | None = None
    data_filters: DataFilters | None = None
    data_storage: DataStorage | None = None
    view_mappings: ViewMappings | None = None
    disciplines: list[Discipline] | None = None
    data_exploration_config: DataExplorationConfig | None = None

    def as_id(self) -> NodeId:
        return NodeId(space=self.space, external_id=self.external_id)
