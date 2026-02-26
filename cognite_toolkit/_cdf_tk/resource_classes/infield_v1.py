from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client.identifiers.identifiers import ExternalId

from .base import BaseModelResource, ToolkitResource


class EnabledToggle(BaseModelResource):
    enabled: bool | None = None


class DisabledToggle(BaseModelResource):
    disabled: bool | None = None


class ThreeDModelIdentifier(BaseModelResource):
    revision_id: int | None = None
    model_id: int | None = None
    name: str | None = None


class ThreeDConfiguration(BaseModelResource):
    full_weight_models: list[ThreeDModelIdentifier] | None = None
    light_weight_models: list[ThreeDModelIdentifier] | None = None


class ResourceFilters(BaseModelResource):
    data_set_external_ids: list[str] | None = None
    asset_subtree_external_ids: list[str] | None = None
    root_asset_external_ids: list[str] | None = None
    external_id_prefix: str | None = None
    spaces: list[str] | None = None


class RootLocationDataFilters(BaseModelResource):
    general: ResourceFilters | None = None
    assets: ResourceFilters | None = None
    files: ResourceFilters | None = None
    timeseries: ResourceFilters | None = None


class ObservationFeatureToggles(BaseModelResource):
    is_enabled: bool | None = None
    is_write_back_enabled: bool | None = None
    notifications_endpoint_external_id: str | None = None
    attachments_endpoint_external_id: str | None = None


class RootLocationFeatureToggles(BaseModelResource):
    three_d: bool | None = None
    trends: bool | None = None
    documents: bool | None = None
    workorders: bool | None = None
    notifications: bool | None = None
    media: bool | None = None
    template_checklist_flow: bool | None = None
    workorder_checklist_flow: bool | None = None
    observations: ObservationFeatureToggles | None = None


class ObservationConfigFieldProperty(BaseModelResource):
    display_title: str | None = None
    display_description: str | None = None
    is_required: bool | None = None


class ObservationConfigDropdownPropertyOption(BaseModelResource):
    id: str | None = None
    value: str | None = None
    label: str | None = None


class ObservationConfigDropdownProperty(ObservationConfigFieldProperty):
    options: list[ObservationConfigDropdownPropertyOption] | None = None


class ObservationsConfig(BaseModelResource):
    files: ObservationConfigFieldProperty | None = None
    description: ObservationConfigFieldProperty | None = None
    asset: ObservationConfigFieldProperty | None = None
    troubleshooting: ObservationConfigFieldProperty | None = None
    type: ObservationConfigDropdownProperty | None = None
    priority: ObservationConfigDropdownProperty | None = None


class RootLocationConfiguration(BaseModelResource):
    asset_external_id: str | None = None
    external_id: str | None = None
    display_name: str | None = None
    three_d_configuration: ThreeDConfiguration | None = None
    data_set_external_id: str | None = None
    template_admins: list[str] | None = None  # list of Group Names
    checklist_admins: list[str] | None = None  # list of Group Names
    app_data_instance_space: str | None = None
    source_data_instance_space: str | None = None
    data_filters: RootLocationDataFilters | None = None
    feature_toggles: RootLocationFeatureToggles | None = None
    observations: ObservationsConfig | None = None


class Activities(BaseModelResource):
    overview_card: dict[str, JsonValue] | None = None


class Documents(BaseModelResource):
    type: str | None = None
    title: str | None = None
    description: str | None = None


class Discipline(BaseModelResource):
    name: str | None = None
    external_id: str | None = None


class Notification(BaseModelResource):
    overview_card: dict[str, JsonValue] | None = None


class CabinetConfiguration(BaseModelResource):
    enable_plan_analysis: bool | None = None
    enable_hourly_optimisation: bool | None = None
    enable_resource_allocation: bool | None = None


class AssetPagePropertyCardConfiguration(BaseModelResource):
    linkable_asset_keys: list[str] | None = None
    highlighted_properties: list[str] | None = None


class AssetPageConfiguration(BaseModelResource):
    property_card: AssetPagePropertyCardConfiguration


class FeatureConfiguration(BaseModelResource):
    root_location_configurations: list[RootLocationConfiguration] | None = None
    copilot: EnabledToggle | DisabledToggle | None = None
    activities: Activities | None = None
    documents: Documents | None = None
    disciplines: list[Discipline] | None = None
    notifications: Notification | None = None
    psn_configuration: EnabledToggle | DisabledToggle | None = None
    canvas_configuration: EnabledToggle | DisabledToggle | None = None
    cabinet_configuration: CabinetConfiguration | None = None
    asset_page_configuration: AssetPageConfiguration | None = None
    subactivities_configuration: EnabledToggle | DisabledToggle | None = None


class InfieldV1YAML(ToolkitResource):
    external_id: str
    name: str | None = None
    app_data_space_id: str | None = None
    app_data_space_version: str | None = None
    customer_data_space_id: str | None = None
    customer_data_space_version: str | None = None
    feature_configuration: FeatureConfiguration | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)
