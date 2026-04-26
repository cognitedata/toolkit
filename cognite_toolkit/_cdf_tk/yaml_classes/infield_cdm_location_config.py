from typing import Literal

from pydantic import Field

from cognite_toolkit._cdf_tk.client.identifiers import NodeId
from cognite_toolkit._cdf_tk.constants import SPACE_FORMAT_PATTERN

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


class ViewMapping(BaseModelResource):
    """View mapping configuration."""

    space: str = Field(min_length=1, max_length=43, pattern=SPACE_FORMAT_PATTERN)
    version: str
    external_id: str
    type: Literal["view"] = "view"


class ViewMappings(BaseModelResource):
    """View mappings configuration."""

    asset: ViewMapping | None = None
    operation: ViewMapping | None = None
    notification: ViewMapping | None = None
    # As of 26/04-26, activity is supported,
    # but InField will likely rename it ot maintenance_order, thus we keep
    # both for now to avoid complaining to the user of either.
    maintenance_order: ViewMapping | None = None
    activity: ViewMapping | None = None

    file: ViewMapping | None = None
    observation: list[ViewMapping] | None = Field(None, min_length=1, max_length=1)


class DataExplorationConfig(BaseModelResource):
    """Data exploration configuration."""

    asset_properties_card: ViewMapping | None = None


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
