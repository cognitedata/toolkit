"""This module contains data classes for the APM Config node used to configure Infield.

These classes are used to represent the configuration of the APM Config node in a structured way, such that it can be
used in the InfieldV1 Loader and thus be represented as a resource type in Toolkit. We do not do any validation in the
FeatureConfiguration objects as this is just JSON object in the node, but use the structure to do lookup of
data sets, spaces, and groups.
"""

import sys
from collections.abc import Sequence
from typing import Any, ClassVar, Literal

import yaml
from cognite.client.data_classes.data_modeling import Node, NodeApply, NodeOrEdgeData, ViewId

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.client.resource_classes.instance_api import (
    TypedNodeIdentifier,
    TypedViewReference,
    WrappedInstanceRequest,
    WrappedInstanceResponse,
)

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


APM_CONFIG_VIEW_ID = TypedViewReference(space="APM_Config", external_id="APM_Config", version="1")


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


class _APMConfigCore(BaseModelObject):
    """Base class for APM Config containing common properties."""

    name: str | None = None
    app_data_space_id: str | None = None
    app_data_space_version: str | None = None
    customer_data_space_id: str | None = None
    customer_data_space_version: str | None = None
    feature_configuration: FeatureConfiguration | None = None

    def dump_yaml(self) -> str:
        """Dump the resource to a YAML string."""
        return yaml.safe_dump(self.dump(camel_case=True), sort_keys=False)

    @classmethod
    def load(cls, data: str | dict[str, Any]) -> Self:
        """Load a resource from a YAML string or dictionary."""
        if isinstance(data, str):
            data = yaml.safe_load(data)
        return cls.model_validate(data)


class APMConfigRequest(WrappedInstanceRequest, _APMConfigCore):
    """APM Config request resource for creating/updating nodes."""

    VIEW_ID: ClassVar[TypedViewReference] = APM_CONFIG_VIEW_ID
    instance_type: Literal["node"] = "node"
    space: str = "APM_Config"

    def as_id(self) -> TypedNodeIdentifier:
        return TypedNodeIdentifier(space=self.space, external_id=self.external_id)

    def as_node(self) -> NodeApply:
        """Convert to a NodeApply for backwards compatibility."""
        # Use dump(context="toolkit") to get all properties, then remove instance-specific fields
        properties = self.dump(camel_case=True, context="toolkit")
        # Remove fields that are not part of the view properties
        for key in ["externalId", "existingVersion", "space", "instanceType"]:
            properties.pop(key, None)
        return NodeApply(
            space=self.space,
            external_id=self.external_id,
            sources=[
                NodeOrEdgeData(
                    source=ViewId(self.VIEW_ID.space, self.VIEW_ID.external_id, self.VIEW_ID.version),
                    properties=properties,
                )
            ],
            existing_version=self.existing_version,
        )

    def dump(
        self, camel_case: bool = True, exclude_extra: bool = False, context: Literal["api", "toolkit"] = "toolkit"
    ) -> dict[str, Any]:
        """Dump the resource to a dictionary.

        Args:
            camel_case (bool): Whether to use camelCase for the keys. Default is True.
            exclude_extra (bool): Whether to exclude extra fields not defined in the model. Default is False.
            context (Literal["api", "toolkit"]): The context in which the dump is used. Default is "toolkit".

        """
        if context == "toolkit":
            # For toolkit format, dump all fields (not using exclude_unset)
            # to preserve data through round-trips
            exclude: set[str] = set()
            if exclude_extra:
                exclude |= set(self.__pydantic_extra__) if self.__pydantic_extra__ else set()
            exclude.update({"instance_type"})
            return self.model_dump(mode="json", by_alias=camel_case, exclude_none=True, exclude=exclude)
        # Use API format from parent class
        return super().dump(camel_case=camel_case, exclude_extra=exclude_extra, context=context)


class APMConfigResponse(WrappedInstanceResponse[APMConfigRequest], _APMConfigCore):
    """APM Config response resource returned from API."""

    VIEW_ID: ClassVar[TypedViewReference] = APM_CONFIG_VIEW_ID
    instance_type: Literal["node"] = "node"
    space: str = "APM_Config"

    def as_request_resource(self) -> APMConfigRequest:
        data = self.dump(camel_case=True)
        # Map response's version to request's existingVersion
        data["existingVersion"] = self.version
        return APMConfigRequest.model_validate(data, extra="ignore")

    def dump(self, camel_case: bool = True, exclude_extra: bool = False) -> dict[str, Any]:
        """Dump the resource to a dictionary in toolkit format."""
        exclude: set[str] = set()
        if exclude_extra:
            exclude |= set(self.__pydantic_extra__) if self.__pydantic_extra__ else set()
        # Exclude instance-specific fields that aren't part of the base properties
        exclude.update({"instance_type", "deleted_time"})
        return self.model_dump(mode="json", by_alias=camel_case, exclude_none=True, exclude=exclude)

    def __eq__(self, other: object) -> bool:
        """Compare two APMConfigResponse objects."""
        if not isinstance(other, APMConfigResponse):
            return NotImplemented
        # Compare all relevant fields, excluding model internals
        return self.dump(camel_case=False) == other.dump(camel_case=False)

    @classmethod
    def from_node(cls, node: Node) -> Self:
        """Load an APMConfigResponse from a data modeling Node."""
        if node.space != "APM_Config":
            raise ValueError(f"Wrong instance space: {node.space}. {cls.__name__} nodes must be in APM_Config space.")

        # Find the view identifier in the node properties
        view_id = ViewId(cls.VIEW_ID.space, cls.VIEW_ID.external_id, cls.VIEW_ID.version)
        view_identifier = {ViewId.load(identifier): identifier for identifier in node.properties.keys()}
        if view_id not in view_identifier:
            raise ValueError(
                f"Missing {cls.__name__} properties. All {cls.__name__} nodes must have properties from {view_id}."
            )
        identifier = view_identifier[view_id]

        # Extract the view properties to top level
        resource = dict(node.properties[identifier])
        resource["instanceType"] = "node"
        resource["space"] = node.space
        resource["externalId"] = node.external_id
        resource["version"] = node.version
        resource["createdTime"] = node.created_time
        resource["lastUpdatedTime"] = node.last_updated_time
        return cls.model_validate(resource)


class APMConfigRequestList(list[APMConfigRequest]):
    def dump(self) -> list[dict[str, Any]]:
        return [item.dump() for item in self]


class APMConfigResponseList(list[APMConfigResponse]):
    def as_request_resources(self) -> APMConfigRequestList:
        return APMConfigRequestList([item.as_request_resource() for item in self])

    @classmethod
    def from_nodes(cls, nodes: Sequence[Node]) -> Self:
        return cls([APMConfigResponse.from_node(item) for item in nodes])


# Backwards compatibility aliases
APMConfigCore = _APMConfigCore
APMConfig = APMConfigResponse
APMConfigWrite = APMConfigRequest
APMConfigWriteList = APMConfigRequestList
APMConfigList = APMConfigResponseList
