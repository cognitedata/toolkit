"""This module contains data classes for the APM Config node used to configure Infield.

These classes are used to represent the configuration of the APM Config node in a structured way, such that it can be
used in the InfieldV1 Loader and thus be represented as a resource type in Toolkit. We do not do any validation in the
FeatureConfiguration objects as this is just JSON object in the node, but use the structure to do lookup of
data sets, spaces, and groups.
"""

import sys
from abc import ABC
from collections.abc import Sequence
from dataclasses import dataclass, field, fields
from functools import lru_cache
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes._base import (
    CogniteObject,
    CogniteResourceList,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.data_classes.data_modeling import Node, NodeApply, NodeApplyList, NodeOrEdgeData, ViewId
from cognite.client.utils._text import to_camel_case

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


@dataclass
class APMConfigObject(CogniteObject):
    """This is used as a base class for all objects in the APM Config node.

    This ensures that all extra fields are  picked up when loading the resource from file and stored in the _extra field.
    When the request is created the dump method is called and the extra fields are added to the request.

    This is done to ensure that all fields are included in the request and that the request is valid in case JSON blob
    in the property featureConfiguration in the view (APM_Config, APM_Config, 1) is changed in the future.
    """

    _extra: dict[str, Any] = field(default_factory=dict, init=False)

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        instance = super()._load(resource, cognite_client=cognite_client)
        instance._extra = {k: v for k, v in resource.items() if k not in cls.get_field_names()}
        return instance

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        if self._extra:
            output.update(self._extra)
        return output

    @classmethod
    @lru_cache(maxsize=1)
    def get_field_names(cls) -> set[str]:
        cls_fields = list(fields(cls))
        return {field.name for field in cls_fields} | {to_camel_case(field.name) for field in cls_fields}


@dataclass
class ThreeDModelIdentifier(APMConfigObject):
    revision_id: int | None = None
    model_id: int | None = None
    name: str | None = None


@dataclass
class ThreeDConfiguration(APMConfigObject):
    full_weight_models: list[ThreeDModelIdentifier] | None = None
    light_weight_models: list[ThreeDModelIdentifier] | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        instance = super()._load(resource, cognite_client=cognite_client)
        for snake, camel in [("full_weight_models", "fullWeightModels"), ("light_weight_models", "lightWeightModels")]:
            if camel in resource:
                setattr(
                    instance,
                    snake,
                    [ThreeDModelIdentifier._load(model, cognite_client=cognite_client) for model in resource[camel]],
                )
        return instance

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        for snake, camel in [("full_weight_models", "fullWeightModels"), ("light_weight_models", "lightWeightModels")]:
            if hasattr(self, snake):
                output[camel if camel_case else snake] = [value.dump() for value in getattr(self, snake) or []]
        return output


@dataclass
class ResourceFilters(APMConfigObject):
    data_set_ids: list[int] | None = None
    asset_subtree_external_ids: list[str] | None = None
    root_asset_external_ids: list[str] | None = None
    external_id_prefix: str | None = None
    spaces: list[str] | None = None


@dataclass
class RootLocationDataFilters(APMConfigObject):
    general: ResourceFilters | None = None
    assets: ResourceFilters | None = None
    files: ResourceFilters | None = None
    timeseries: ResourceFilters | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        instance = super()._load(resource, cognite_client=cognite_client)
        for key in ["general", "assets", "files", "timeseries"]:
            if key in resource:
                setattr(instance, key, ResourceFilters._load(resource[key], cognite_client=cognite_client))
        return instance

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        for key in ["general", "assets", "files", "timeseries"]:
            if hasattr(self, key):
                if value := getattr(self, key):
                    output[key] = value.dump(camel_case=camel_case)
        return output


@dataclass
class ObservationFeatureToggles(APMConfigObject):
    is_enabled: bool | None = None
    is_write_back_enabled: bool | None = None
    notifications_endpoint_external_id: str | None = None
    attachments_endpoint_external_id: str | None = None


@dataclass
class RootLocationFeatureToggles(APMConfigObject):
    three_d: bool | None = None
    trends: bool | None = None
    documents: bool | None = None
    workorders: bool | None = None
    notifications: bool | None = None
    media: bool | None = None
    template_checklist_flow: bool | None = None
    workorder_checklist_flow: bool | None = None
    observations: ObservationFeatureToggles | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        instance = super()._load(resource, cognite_client=cognite_client)
        if "observations" in resource:
            instance.observations = ObservationFeatureToggles._load(
                resource["observations"], cognite_client=cognite_client
            )
        return instance

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        if self.observations:
            output["observations"] = self.observations.dump(camel_case=camel_case)
        return output


@dataclass
class ObservationConfigFieldProperty(APMConfigObject):
    display_title: str | None = None
    display_description: str | None = None
    is_required: bool | None = None


@dataclass
class ObservationConfigDropdownPropertyOption(APMConfigObject):
    id: str | None = None
    value: str | None = None
    label: str | None = None


@dataclass
class ObservationConfigDropdownProperty(ObservationConfigFieldProperty):
    options: list[ObservationConfigDropdownPropertyOption] | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        instance = super()._load(resource, cognite_client=cognite_client)
        if "options" in resource:
            instance.options = [
                ObservationConfigDropdownPropertyOption._load(option, cognite_client=cognite_client)
                for option in resource["options"]
            ]
        return instance

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        if self.options:
            output["options"] = [option.dump(camel_case=camel_case) for option in self.options]
        return output


@dataclass
class ObservationsConfig(APMConfigObject):
    files: ObservationConfigFieldProperty | None = None
    description: ObservationConfigFieldProperty | None = None
    asset: ObservationConfigFieldProperty | None = None
    troubleshooting: ObservationConfigFieldProperty | None = None
    type: ObservationConfigDropdownProperty | None = None
    priority: ObservationConfigDropdownProperty | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        instance = super()._load(resource, cognite_client=cognite_client)
        for key in ["files", "description", "asset", "troubleshooting"]:
            if key in resource:
                setattr(
                    instance, key, ObservationConfigFieldProperty._load(resource[key], cognite_client=cognite_client)
                )
        for key in ["type", "priority"]:
            if key in resource:
                setattr(
                    instance, key, ObservationConfigDropdownProperty._load(resource[key], cognite_client=cognite_client)
                )
        return instance

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        for key in ["files", "description", "asset", "troubleshooting", "type", "priority"]:
            if hasattr(self, key):
                if value := getattr(self, key):
                    output[key] = value.dump(camel_case=camel_case)
        return output


@dataclass
class RootLocationConfiguration(APMConfigObject):
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

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        instance = super()._load(resource, cognite_client=cognite_client)
        if "threeDConfiguration" in resource:
            instance.three_d_configuration = ThreeDConfiguration._load(
                resource["threeDConfiguration"], cognite_client=cognite_client
            )
        if "dataFilters" in resource:
            instance.data_filters = RootLocationDataFilters._load(
                resource["dataFilters"], cognite_client=cognite_client
            )
        if "featureToggles" in resource:
            instance.feature_toggles = RootLocationFeatureToggles._load(
                resource["featureToggles"], cognite_client=cognite_client
            )
        if "observations" in resource:
            instance.observations = ObservationsConfig._load(resource["observations"], cognite_client=cognite_client)
        return instance

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        if self.three_d_configuration:
            output["threeDConfiguration" if camel_case else "three_d_configuration"] = self.three_d_configuration.dump(
                camel_case=camel_case
            )
        if self.data_filters:
            output["dataFilters" if camel_case else "data_filters"] = self.data_filters.dump(camel_case=camel_case)
        if self.feature_toggles:
            output["featureToggles" if camel_case else "feature_toggles"] = self.feature_toggles.dump(
                camel_case=camel_case
            )
        if self.observations:
            output["observations"] = self.observations.dump(camel_case=camel_case)
        return output


@dataclass
class FeatureConfiguration(APMConfigObject):
    root_location_configurations: list[RootLocationConfiguration] | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        instance = super()._load(resource, cognite_client=cognite_client)
        if "rootLocationConfigurations" in resource:
            instance.root_location_configurations = [
                RootLocationConfiguration._load(item, cognite_client=cognite_client)
                for item in resource.get("rootLocationConfigurations", [])
            ]
        return instance

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        if self.root_location_configurations:
            output["rootLocationConfigurations" if camel_case else "root_location_configurations"] = [
                item.dump(camel_case=camel_case) for item in self.root_location_configurations
            ]
        return output


class APMConfigCore(WriteableCogniteResource["APMConfigWrite"], ABC):
    space: str = "APM_Config"
    view_id: ViewId = ViewId("APM_Config", "APM_Config", "1")

    def __init__(
        self,
        external_id: str,
        name: str | None = None,
        app_data_space_id: str | None = None,
        app_data_space_version: str | None = None,
        customer_data_space_id: str | None = None,
        customer_data_space_version: str | None = None,
        feature_configuration: FeatureConfiguration | None = None,
    ) -> None:
        self.external_id = external_id
        self.name = name
        self.app_data_space_id = app_data_space_id
        self.app_data_space_version = app_data_space_version
        self.customer_data_space_id = customer_data_space_id
        self.customer_data_space_version = customer_data_space_version
        self.feature_configuration = feature_configuration

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        if self.feature_configuration:
            output["featureConfiguration" if camel_case else "feature_configuration"] = self.feature_configuration.dump(
                camel_case=camel_case
            )
        return output


class APMConfigWrite(APMConfigCore):
    def __init__(
        self,
        external_id: str,
        name: str | None = None,
        app_data_space_id: str | None = None,
        app_data_space_version: str | None = None,
        customer_data_space_id: str | None = None,
        customer_data_space_version: str | None = None,
        feature_configuration: FeatureConfiguration | None = None,
        existing_version: int | None = None,
    ) -> None:
        super().__init__(
            external_id=external_id,
            name=name,
            app_data_space_id=app_data_space_id,
            app_data_space_version=app_data_space_version,
            customer_data_space_id=customer_data_space_id,
            customer_data_space_version=customer_data_space_version,
            feature_configuration=feature_configuration,
        )
        self.existing_version = existing_version

    def as_write(self) -> "APMConfigWrite":
        return self

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            external_id=resource["externalId"],
            name=resource.get("name"),
            app_data_space_id=resource.get("appDataSpaceId"),
            app_data_space_version=resource.get("appDataSpaceVersion"),
            customer_data_space_id=resource.get("customerDataSpaceId"),
            customer_data_space_version=resource.get("customerDataSpaceVersion"),
            feature_configuration=FeatureConfiguration._load(
                resource["featureConfiguration"], cognite_client=cognite_client
            )
            if "featureConfiguration" in resource
            else None,
            existing_version=resource.get("existingVersion"),
        )

    def as_node(self) -> NodeApply:
        properties = self.dump(camel_case=True)
        # Node properties.
        properties.pop("externalId", None)
        properties.pop("existingVersion", None)
        return NodeApply(
            space=self.space,
            external_id=self.external_id,
            sources=[
                NodeOrEdgeData(
                    source=self.view_id,
                    properties=properties,
                )
            ],
            existing_version=self.existing_version,
        )


class APMConfig(APMConfigCore):
    def __init__(
        self,
        external_id: str,
        version: int,
        created_time: int,
        last_updated_time: int,
        name: str | None = None,
        app_data_space_id: str | None = None,
        app_data_space_version: str | None = None,
        customer_data_space_id: str | None = None,
        customer_data_space_version: str | None = None,
        feature_configuration: FeatureConfiguration | None = None,
    ) -> None:
        super().__init__(
            external_id=external_id,
            name=name,
            app_data_space_id=app_data_space_id,
            app_data_space_version=app_data_space_version,
            customer_data_space_id=customer_data_space_id,
            customer_data_space_version=customer_data_space_version,
            feature_configuration=feature_configuration,
        )
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.version = version

    def as_write(self) -> APMConfigWrite:
        return APMConfigWrite(
            external_id=self.external_id,
            name=self.name,
            app_data_space_id=self.app_data_space_id,
            app_data_space_version=self.app_data_space_version,
            customer_data_space_id=self.customer_data_space_id,
            customer_data_space_version=self.customer_data_space_version,
            feature_configuration=self.feature_configuration,
            existing_version=self.version,
        )

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            external_id=resource["externalId"],
            version=resource["version"],
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
            name=resource.get("name"),
            app_data_space_id=resource.get("appDataSpaceId"),
            app_data_space_version=resource.get("appDataSpaceVersion"),
            customer_data_space_id=resource.get("customerDataSpaceId"),
            customer_data_space_version=resource.get("customerDataSpaceVersion"),
            feature_configuration=FeatureConfiguration._load(
                resource["featureConfiguration"], cognite_client=cognite_client
            )
            if "featureConfiguration" in resource
            else None,
        )

    @classmethod
    def from_node(cls, node: Node) -> Self:
        if node.space != cls.space:
            raise ValueError(f"Wrong instance space: {node.space}. {cls.__name__} nodes must be in {cls.space} space.")
        view_identifier = {ViewId.load(identifier): identifier for identifier in node.properties.keys()}
        if cls.view_id not in view_identifier:
            raise ValueError(
                f"Missing {cls.__name__} properties. All {cls.__name__} nodes must have properties from {cls.view_id}."
            )
        identifier = view_identifier[cls.view_id]

        resource = dict(node.properties[identifier])
        resource["externalId"] = node.external_id
        resource["createdTime"] = node.created_time
        resource["lastUpdatedTime"] = node.last_updated_time
        resource["version"] = node.version
        return cls._load(resource, None)


class APMConfigWriteList(CogniteResourceList[APMConfigWrite]):
    _RESOURCE = APMConfigWrite

    def as_nodes(self) -> NodeApplyList:
        return NodeApplyList([item.as_node() for item in self])


class APMConfigList(WriteableCogniteResourceList[APMConfigWrite, APMConfig]):
    _RESOURCE = APMConfig

    def as_write(self) -> APMConfigWriteList:
        return APMConfigWriteList([item.as_write() for item in self])

    @classmethod
    def from_nodes(cls, node: Sequence[Node]) -> Self:
        return cls([cls._RESOURCE.from_node(item) for item in node], cognite_client=None)
