"""This module contains data classes for the APM Config node used to configure Infield.

These classes are used to represent the configuration of the APM Config node in a structured way, such that it can be
used in the InfieldV1 Loader and thus be represented as a resource type in Toolkit. We do not do any validation in the
FeatureConfiguration objects as this is just JSON object in the node, but use the structure to do lookup of
data sets, spaces, and groups.
"""

import sys
from typing import ClassVar, Literal

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.client.resource_classes.instance_api import (
    TypedNodeIdentifier,
    TypedViewReference,
    WrappedInstanceRequest,
    WrappedInstanceResponse,
)

if sys.version_info >= (3, 11):
    pass
else:
    pass

APM_CONFIG_SPACE: Literal["APM_Config"] = "APM_Config"
APM_CONFIG_VIEW_ID = TypedViewReference(space=APM_CONFIG_SPACE, external_id="APM_Config", version="1")


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
    """Base class for APM Config containing common properties."""

    name: str | None = None
    app_data_space_id: str | None = None
    app_data_space_version: str | None = None
    customer_data_space_id: str | None = None
    customer_data_space_version: str | None = None
    feature_configuration: FeatureConfiguration | None = None


class APMConfigRequest(WrappedInstanceRequest, APMConfig):
    """APM Config request resource for creating/updating nodes."""

    VIEW_ID: ClassVar[TypedViewReference] = APM_CONFIG_VIEW_ID
    instance_type: Literal["node"] = "node"
    space: Literal["APM_Config"] = APM_CONFIG_SPACE

    def as_id(self) -> TypedNodeIdentifier:
        return TypedNodeIdentifier(space=self.space, external_id=self.external_id)


class APMConfigResponse(WrappedInstanceResponse[APMConfigRequest], APMConfig):
    """APM Config response resource returned from API."""

    VIEW_ID: ClassVar[TypedViewReference] = APM_CONFIG_VIEW_ID
    instance_type: Literal["node"] = "node"
    space: Literal["APM_Config"] = APM_CONFIG_SPACE

    def as_request_resource(self) -> APMConfigRequest:
        return APMConfigRequest.model_validate(self.dump(), extra="ignore")
