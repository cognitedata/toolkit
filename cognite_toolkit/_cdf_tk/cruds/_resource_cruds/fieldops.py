import collections.abc
from collections.abc import Hashable, Iterable, Sequence, Sized
from pathlib import Path
from typing import Any, final

from cognite.client.data_classes.capabilities import Capability, DataModelInstancesAcl
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.resource_classes.apm_config_v1 import (
    APM_CONFIG_SPACE,
    APMConfigRequest,
    APMConfigResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import SpaceReference
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling._instance import InstanceSlimDefinition
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExternalId, NameId
from cognite_toolkit._cdf_tk.client.resource_classes.infield import (
    InFieldCDMLocationConfigRequest,
    InFieldCDMLocationConfigResponse,
    InFieldLocationConfigRequest,
    InFieldLocationConfigResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.instance_api import TypedNodeIdentifier
from cognite_toolkit._cdf_tk.constants import BUILD_FOLDER_ENCODING
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceCRUD
from cognite_toolkit._cdf_tk.resource_classes import (
    InFieldCDMLocationConfigYAML,
    InfieldLocationConfigYAML,
    InfieldV1YAML,
)
from cognite_toolkit._cdf_tk.utils import quote_int_value_by_key_in_yaml, safe_read
from cognite_toolkit._cdf_tk.utils.diff_list import diff_list_hashable, diff_list_identifiable, hash_dict

from .auth import GroupAllScopedCRUD
from .classic import AssetCRUD
from .data_organization import DataSetsCRUD
from .datamodel import SpaceCRUD, ViewCRUD
from .group_scoped import GroupResourceScopedCRUD


@final
class InfieldV1CRUD(ResourceCRUD[ExternalId, APMConfigRequest, APMConfigResponse]):
    folder_name = "cdf_applications"
    resource_cls = APMConfigResponse
    resource_write_cls = APMConfigRequest
    kind = "InfieldV1"
    yaml_cls = InfieldV1YAML
    dependencies = frozenset({DataSetsCRUD, AssetCRUD, SpaceCRUD, GroupAllScopedCRUD, GroupResourceScopedCRUD})
    _doc_url = "Instances/operation/applyNodeAndEdges"
    _root_location_filters: tuple[str, ...] = ("general", "assets", "files", "timeseries")
    _group_keys: tuple[str, ...] = ("templateAdmins", "checklistAdmins")

    @property
    def display_name(self) -> str:
        return "infield configs"

    @classmethod
    def get_id(cls, item: APMConfigResponse | APMConfigRequest | dict) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        if not item.external_id:
            raise KeyError("APMConfig must have external_id")
        return ExternalId(external_id=item.external_id)

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return {"externalId": id.external_id}

    @classmethod
    def get_required_capability(
        cls, items: collections.abc.Sequence[APMConfigRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []

        actions = (
            [DataModelInstancesAcl.Action.Read]
            if read_only
            else [DataModelInstancesAcl.Action.Read, DataModelInstancesAcl.Action.Write]
        )

        return DataModelInstancesAcl(actions, DataModelInstancesAcl.Scope.SpaceID([APM_CONFIG_SPACE]))

    def prerequisite_warning(self) -> str | None:
        view_id = APMConfigRequest.VIEW_ID
        views = self.client.data_modeling.views.retrieve((view_id.space, view_id.external_id, view_id.version))
        if len(views) > 0:
            return None
        return (
            f"{self.display_name} requires the {APMConfigRequest.VIEW_ID!r} to be deployed. "
            f"Install the infield options with cdf modules init/add to deploy it."
        )

    def create(self, items: Sequence[APMConfigRequest]) -> list[InstanceSlimDefinition]:
        return self.client.infield.apm_config.create(items)

    def retrieve(self, ids: SequenceNotStr[ExternalId]) -> list[APMConfigResponse]:
        return self.client.infield.apm_config.retrieve(
            TypedNodeIdentifier.from_external_ids(ids, space=APM_CONFIG_SPACE)
        )

    def update(self, items: Sequence[APMConfigRequest]) -> list[InstanceSlimDefinition]:
        return self.client.infield.apm_config.create(items)

    def delete(self, ids: SequenceNotStr[ExternalId]) -> int:
        deleted = self.client.infield.apm_config.delete(
            TypedNodeIdentifier.from_external_ids(ids, space=APM_CONFIG_SPACE)
        )
        return len(deleted)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[APMConfigResponse]:
        raise NotImplementedError(f"Iteration over {self.display_name} is not supported.")

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceCRUD], Hashable]]:
        if isinstance(app_data_space_id := item.get("appDataSpaceId"), str):
            yield SpaceCRUD, SpaceReference(space=app_data_space_id)
        if isinstance(customer_data_space_id := item.get("customerDataSpaceId"), str):
            yield SpaceCRUD, SpaceReference(space=customer_data_space_id)
        for config in cls._get_root_location_configurations(item) or []:
            if isinstance(asset_external_id := config.get("assetExternalId"), str):
                yield AssetCRUD, ExternalId(external_id=asset_external_id)
            if isinstance(data_set_external_id := config.get("dataSetExternalId"), str):
                yield DataSetsCRUD, ExternalId(external_id=data_set_external_id)
            if isinstance(app_data_instance_space := config.get("appDataInstanceSpace"), str):
                yield SpaceCRUD, SpaceReference(space=app_data_instance_space)
            if isinstance(source_data_instance_space := config.get("sourceDataInstanceSpace"), str):
                yield SpaceCRUD, SpaceReference(space=source_data_instance_space)
            for key in cls._group_keys:
                for group in config.get(key, []):
                    if isinstance(group, str):
                        yield GroupResourceScopedCRUD, NameId(name=group)
            data_filters = config.get("dataFilters")
            if not isinstance(data_filters, dict):
                continue
            for key in cls._root_location_filters:
                filter_ = data_filters.get(key)
                if not isinstance(filter_, dict):
                    continue
                for data_set_external_id in filter_.get("dataSetExternalIds", []):
                    if isinstance(data_set_external_id, str):
                        yield DataSetsCRUD, ExternalId(external_id=data_set_external_id)
                for asset_external_id in filter_.get("assetSubtreeExternalIds", []):
                    if isinstance(asset_external_id, str):
                        yield AssetCRUD, ExternalId(external_id=asset_external_id)
                if app_data_instance_space := filter_.get("appDataInstanceSpace"):
                    if isinstance(app_data_instance_space, str):
                        yield SpaceCRUD, SpaceReference(space=app_data_instance_space)

    def safe_read(self, filepath: Path | str) -> str:
        # The customerDataSpaceVersion is a string, but the user often writes it as an int.
        # YAML will then parse it as an int, for example, `3_0_2` will be parsed as `302`.
        # This is technically a user mistake, as you should quote the version in the YAML file.
        # However, we do not want to put this burden on the user (knowing the intricate workings of YAML),
        # so we fix it here.
        return quote_int_value_by_key_in_yaml(
            safe_read(filepath, encoding=BUILD_FOLDER_ENCODING), key="customerDataSpaceVersion"
        )

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> APMConfigRequest:
        root_location_configurations = self._get_root_location_configurations(resource)
        for config in root_location_configurations or []:
            if not isinstance(config, dict):
                continue
            if ds_external_id := config.pop("dataSetExternalId", None):
                config["dataSetId"] = self.client.lookup.data_sets.id(ds_external_id, is_dry_run)
            data_filters = config.get("dataFilters")
            if not isinstance(data_filters, dict):
                continue
            for key in self._root_location_filters:
                filter_ = data_filters.get(key)
                if not isinstance(filter_, dict):
                    continue
                if ds_external_ids := filter_.pop("dataSetExternalIds", None):
                    filter_["dataSetIds"] = self.client.lookup.data_sets.id(ds_external_ids, is_dry_run)
        return APMConfigRequest._load(resource)

    def dump_resource(self, resource: APMConfigResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_write().dump(context="toolkit")
        local = local or {}
        if "existingVersion" not in local:
            # Existing version is typically not set when creating nodes, but we get it back
            # when we retrieve the node from the server.
            dumped.pop("existingVersion", None)

        for key in ("space", "instanceType"):
            if key in dumped and key not in local:
                # space and instanceType are required when interacting with the API,
                # but not when defining the resource locally in YAML.
                dumped.pop(key)

        for config in self._get_root_location_configurations(dumped) or []:
            if not isinstance(config, dict):
                continue
            if data_set_id := config.pop("dataSetId", None):
                config["dataSetExternalId"] = self.client.lookup.data_sets.external_id(data_set_id)
            data_filters = config.get("dataFilters")
            if not isinstance(data_filters, dict):
                continue
            for key in self._root_location_filters:
                filter_ = data_filters.get(key)
                if not isinstance(filter_, dict):
                    continue
                if data_set_ids := filter_.pop("dataSetIds", None):
                    filter_["dataSetExternalIds"] = self.client.lookup.data_sets.external_id(data_set_ids)
        return dumped

    def diff_list(
        self, local: list[Any], cdf: list[Any], json_path: tuple[str | int, ...]
    ) -> tuple[dict[int, int], list[int]]:
        if json_path == ("featureConfiguration", "rootLocationConfigurations"):
            return diff_list_identifiable(local, cdf, get_identifier=hash_dict)
        if not (len(json_path) >= 3 and json_path[:2] == ("featureConfiguration", "rootLocationConfigurations")):
            return super().diff_list(local, cdf, json_path)

        if len(json_path) == 4 and json_path[-1] in self._group_keys:
            return diff_list_hashable(local, cdf)
        if len(json_path) == 5 and json_path[-1] in ("fullWeightModels", "lightWeightModels"):
            return diff_list_identifiable(local, cdf, get_identifier=hash_dict)
        if len(json_path) == 6 and json_path[-2] == "dataFilters" and json_path[-1] in self._root_location_filters:
            return diff_list_hashable(local, cdf)
        if len(json_path) == 7 and "observations" in json_path and json_path[-1] in ("type", "priority"):
            return diff_list_identifiable(local, cdf, get_identifier=hash_dict)
        return super().diff_list(local, cdf, json_path)

    @staticmethod
    def _get_root_location_configurations(resource: dict[str, Any]) -> list | None:
        feature_configuration = resource.get("featureConfiguration")
        if not isinstance(feature_configuration, dict):
            return None
        return feature_configuration.get("rootLocationConfigurations")


@final
class InFieldLocationConfigCRUD(
    ResourceCRUD[TypedNodeIdentifier, InFieldLocationConfigRequest, InFieldLocationConfigResponse]
):
    folder_name = "cdf_applications"
    resource_cls = InFieldLocationConfigResponse
    resource_write_cls = InFieldLocationConfigRequest
    kind = "InFieldLocationConfig"
    yaml_cls = InfieldLocationConfigYAML
    dependencies = frozenset({SpaceCRUD, GroupAllScopedCRUD, GroupResourceScopedCRUD})
    _doc_url = "Instances/operation/applyNodeAndEdges"

    @property
    def display_name(self) -> str:
        return "infield location configs"

    @classmethod
    def get_id(cls, item: InFieldLocationConfigRequest | InFieldLocationConfigResponse | dict) -> TypedNodeIdentifier:
        if isinstance(item, dict):
            return TypedNodeIdentifier(space=item["space"], external_id=item["externalId"])
        return TypedNodeIdentifier(space=item.space, external_id=item.external_id)

    @classmethod
    def dump_id(cls, id: TypedNodeIdentifier) -> dict[str, Any]:
        return {
            "space": id.space,
            "externalId": id.external_id,
        }

    @classmethod
    def get_required_capability(
        cls, items: Sequence[InFieldLocationConfigRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items or items is None:
            return []

        actions = (
            [DataModelInstancesAcl.Action.Read]
            if read_only
            else [DataModelInstancesAcl.Action.Read, DataModelInstancesAcl.Action.Write]
        )
        instance_spaces = sorted({item.space for item in items})

        return DataModelInstancesAcl(actions, DataModelInstancesAcl.Scope.SpaceID(instance_spaces))

    def dump_resource(
        self, resource: InFieldLocationConfigResponse, local: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        dumped = resource.as_request_resource().dump()
        local = local or {}
        dumped.pop("instanceType", None)
        if isinstance(cdf_dec := dumped.get("dataExplorationConfig"), dict):
            cdf_dec.pop("instanceType", None)
            if isinstance(local_dec := local.get("dataExplorationConfig"), dict):
                if "space" in cdf_dec and "space" not in local_dec:
                    # Default space is used for the data exploration config if not specified locally.
                    cdf_dec.pop("space")
                if "externalId" in cdf_dec and "externalId" not in local_dec:
                    # Default externalId is used for the data exploration config if not specified locally.
                    cdf_dec.pop("externalId")

        return dumped

    def create(self, items: Sequence[InFieldLocationConfigRequest]) -> list[InstanceSlimDefinition]:
        return self.client.infield.config.create(items)

    def retrieve(self, ids: SequenceNotStr[TypedNodeIdentifier]) -> list[InFieldLocationConfigResponse]:
        return self.client.infield.config.retrieve(list(ids))

    def update(self, items: Sequence[InFieldLocationConfigRequest]) -> Sized:
        return self.client.infield.config.update(items)

    def delete(self, ids: SequenceNotStr[TypedNodeIdentifier]) -> int:
        return len(self.client.infield.config.delete(list(ids)))

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[InFieldLocationConfigResponse]:
        raise NotImplementedError(f"Iteration over {self.display_name} is not supported.")

    def diff_list(
        self, local: list[Any], cdf: list[Any], json_path: tuple[str | int, ...]
    ) -> tuple[dict[int, int], list[int]]:
        if json_path == ("accessManagement", "templateAdmins"):
            return diff_list_hashable(local, cdf)
        elif json_path == ("accessManagement", "checklistAdmins"):
            return diff_list_hashable(local, cdf)
        elif json_path == ("dataFilters", "general", "spaces"):
            return diff_list_hashable(local, cdf)
        elif json_path == ("dataExplorationConfig", "documents", "supportedFormats"):
            return diff_list_hashable(local, cdf)
        return super().diff_list(local, cdf, json_path)


@final
class InFieldCDMLocationConfigCRUD(
    ResourceCRUD[TypedNodeIdentifier, InFieldCDMLocationConfigRequest, InFieldCDMLocationConfigResponse]
):
    folder_name = "cdf_applications"
    resource_cls = InFieldCDMLocationConfigResponse
    resource_write_cls = InFieldCDMLocationConfigRequest
    kind = "InFieldCDMLocationConfig"
    yaml_cls = InFieldCDMLocationConfigYAML
    dependencies = frozenset({SpaceCRUD, GroupAllScopedCRUD, GroupResourceScopedCRUD, ViewCRUD})
    _doc_url = "Instances/operation/applyNodeAndEdges"

    @property
    def display_name(self) -> str:
        return "infield CDM location configs"

    @classmethod
    def get_id(
        cls, item: InFieldCDMLocationConfigRequest | InFieldCDMLocationConfigResponse | dict
    ) -> TypedNodeIdentifier:
        if isinstance(item, dict):
            return TypedNodeIdentifier(space=item["space"], external_id=item["externalId"])
        return TypedNodeIdentifier(space=item.space, external_id=item.external_id)

    @classmethod
    def dump_id(cls, id: TypedNodeIdentifier) -> dict[str, Any]:
        return {
            "space": id.space,
            "externalId": id.external_id,
        }

    @classmethod
    def get_required_capability(
        cls, items: Sequence[InFieldCDMLocationConfigRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items or items is None:
            return []

        actions = (
            [DataModelInstancesAcl.Action.Read]
            if read_only
            else [DataModelInstancesAcl.Action.Read, DataModelInstancesAcl.Action.Write]
        )
        instance_spaces = sorted({item.space for item in items})

        return DataModelInstancesAcl(actions, DataModelInstancesAcl.Scope.SpaceID(instance_spaces))

    def dump_resource(
        self, resource: InFieldCDMLocationConfigResponse, local: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        dumped = resource.as_request_resource().dump(context="toolkit")
        local = local or {}
        if "existingVersion" not in local:
            # Existing version is typically not set when creating nodes, but we get it back
            # when we retrieve the node from the server.
            dumped.pop("existingVersion", None)
        dumped.pop("instanceType", None)
        return dumped

    def create(self, items: Sequence[InFieldCDMLocationConfigRequest]) -> list[InstanceSlimDefinition]:
        return self.client.infield.cdm_config.create(items)

    def retrieve(self, ids: SequenceNotStr[TypedNodeIdentifier]) -> list[InFieldCDMLocationConfigResponse]:
        return self.client.infield.cdm_config.retrieve(list(ids))

    def update(self, items: Sequence[InFieldCDMLocationConfigRequest]) -> Sized:
        return self.create(items)

    def delete(self, ids: SequenceNotStr[TypedNodeIdentifier]) -> int:
        deleted = self.client.infield.cdm_config.delete(list(ids))
        return len(deleted)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[InFieldCDMLocationConfigResponse]:
        raise NotImplementedError(f"Iteration over {self.display_name} is not supported.")

    def diff_list(
        self, local: list[Any], cdf: list[Any], json_path: tuple[str | int, ...]
    ) -> tuple[dict[int, int], list[int]]:
        if json_path == ("accessManagement", "templateAdmins"):
            return diff_list_hashable(local, cdf)
        elif json_path == ("accessManagement", "checklistAdmins"):
            return diff_list_hashable(local, cdf)
        elif json_path == ("disciplines",):
            return diff_list_identifiable(local, cdf, get_identifier=hash_dict)
        elif len(json_path) == 3 and json_path[0] == "dataFilters" and json_path[2] == "instanceSpaces":
            # Handles dataFilters.<entity>.instanceSpaces (e.g., files, assets, operations, timeSeries, etc.)
            return diff_list_hashable(local, cdf)
        elif json_path == ("dataExplorationConfig", "filters"):
            return diff_list_identifiable(local, cdf, get_identifier=hash_dict)
        elif len(json_path) == 4 and json_path[:2] == ("dataExplorationConfig", "filters") and json_path[3] == "values":
            # Handles dataExplorationConfig.filters[i].values
            return diff_list_hashable(local, cdf)

        return super().diff_list(local, cdf, json_path)
