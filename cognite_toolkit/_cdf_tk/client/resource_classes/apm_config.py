from typing import ClassVar, Literal

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    RequestResource,
    ResponseResource,
)

from .data_modeling import ViewReference
from .identifiers import ExternalId


class ThreeDModelIdentifier(BaseModelObject):
    revision_id: int | None = None
    model_id: int | None = None
    name: str | None = None


class ThreeDConfiguration(BaseModelObject):
    full_weight_models: list[ThreeDModelIdentifier] | None = None
    light_weight_models: list[ThreeDModelIdentifier] | None = None


class ResourceFilters(BaseModelObject):
    data_set_ids: list[int] | None = None
    asset_subtree_external_ids: list[str] | None = None
    root_asset_external_ids: list[str] | None = None
    external_id_prefix: str | None = None
    spaces: list[str] | None = None


class RootLocationDataFilters(BaseModelObject):
    general: ResourceFilters | None = None
    assets: ResourceFilters | None = None
    files: ResourceFilters | None = None
    timeseries: ResourceFilters | None = None


class ObservationFeatureToggles(BaseModelObject):
    is_enabled: bool | None = None
    is_write_back_enabled: bool | None = None
    notifications_endpoint_external_id: str | None = None
    attachments_endpoint_external_id: str | None = None


class RootLocationFeatureToggles(BaseModelObject):
    three_d: bool | None = None
    trends: bool | None = None
    documents: bool | None = None
    workorders: bool | None = None
    notifications: bool | None = None
    media: bool | None = None
    template_checklist_flow: bool | None = None
    workorder_checklist_flow: bool | None = None
    observations: ObservationFeatureToggles | None = None


class ObservationConfigFieldProperty(BaseModelObject):
    display_title: str | None = None
    display_description: str | None = None
    is_required: bool | None = None


class ObservationConfigDropdownPropertyOption(BaseModelObject):
    id: str | None = None
    value: str | None = None
    label: str | None = None


class ObservationConfigDropdownProperty(ObservationConfigFieldProperty):
    options: list[ObservationConfigDropdownPropertyOption] | None = None


class ObservationsConfig(BaseModelObject):
    files: ObservationConfigFieldProperty | None = None
    description: ObservationConfigFieldProperty | None = None
    asset: ObservationConfigFieldProperty | None = None
    troubleshooting: ObservationConfigFieldProperty | None = None
    type: ObservationConfigDropdownProperty | None = None
    priority: ObservationConfigDropdownProperty | None = None


class RootLocationConfiguration(BaseModelObject):
    asset_external_id: str | None = None
    external_id: str | None = None
    display_name: str | None = None
    three_d_configuration: ThreeDConfiguration | None = None
    data_set_id: int | None = None
    template_admins: list[str] | None = None  # list of Group Names
    checklist_admins: list[str] | None = None  # list of Group Names
    app_data_instance_space: str | None = None
    source_data_instance_space: str | None = None
    data_filters: RootLocationDataFilters | None = None
    feature_toggles: RootLocationFeatureToggles | None = None
    observations: ObservationsConfig | None = None


class FeatureConfiguration(BaseModelObject):
    root_location_configurations: list[RootLocationConfiguration] | None = None


class APMConfig(BaseModelObject):
    space: ClassVar[str] = "APM_Config"
    view_ref: ClassVar[ViewReference] = ViewReference(space="APM_Config", external_id="APM_Config", version="1")
    instance_type: Literal["node"] = "node"
    external_id: str
    name: str | None = None
    app_data_space_id: str | None = None
    app_data_space_version: str | None = None
    customer_data_space_id: str | None = None
    customer_data_space_version: str | None = None
    feature_configuration: FeatureConfiguration | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class APMConfigRequest(APMConfig, RequestResource):
    existing_version: int | None = None


class APMConfigResponse(APMConfig, ResponseResource[APMConfigRequest]):
    version: int
    created_time: int
    last_updated_time: int

    def as_request_resource(self) -> APMConfigRequest:
        return APMConfigRequest.model_validate(self.dump(), extra="ignore")
