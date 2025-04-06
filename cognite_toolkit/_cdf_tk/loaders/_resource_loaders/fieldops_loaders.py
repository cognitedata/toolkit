import collections.abc
from collections.abc import Hashable, Iterable
from functools import lru_cache
from pathlib import Path
from typing import Any, final

from cognite.client.data_classes.capabilities import Capability, DataModelInstancesAcl
from cognite.client.data_classes.data_modeling import NodeApplyResultList, NodeId
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk._parameters import ParameterSpec, ParameterSpecSet
from cognite_toolkit._cdf_tk.client.data_classes.apm_config_v1 import (
    APMConfig,
    APMConfigList,
    APMConfigWrite,
    APMConfigWriteList,
)
from cognite_toolkit._cdf_tk.loaders._base_loaders import ResourceLoader
from cognite_toolkit._cdf_tk.utils import quote_int_value_by_key_in_yaml, safe_read
from cognite_toolkit._cdf_tk.utils.cdf import iterate_instances
from cognite_toolkit._cdf_tk.utils.diff_list import diff_list_hashable, diff_list_identifiable, hash_dict

from .auth_loaders import GroupAllScopedLoader
from .classic_loaders import AssetLoader
from .data_organization_loaders import DataSetsLoader
from .datamodel_loaders import SpaceLoader
from .group_scoped_loader import GroupResourceScopedLoader


@final
class InfieldV1Loader(ResourceLoader[str, APMConfigWrite, APMConfig, APMConfigWriteList, APMConfigList]):
    folder_name = "cdf_applications"
    filename_pattern = r"^.*\.InfieldV1$"  # Matches all yaml files whose stem ends with '.InfieldV1'.
    filetypes = frozenset({"yaml", "yml"})
    resource_cls = APMConfig
    resource_write_cls = APMConfigWrite
    list_cls = APMConfigList
    list_write_cls = APMConfigWriteList
    kind = "InfieldV1"
    dependencies = frozenset(
        {DataSetsLoader, AssetLoader, SpaceLoader, GroupAllScopedLoader, GroupResourceScopedLoader}
    )
    _doc_url = "Instances/operation/applyNodeAndEdges"
    _root_location_filters: tuple[str, ...] = ("general", "assets", "files", "timeseries")
    _group_keys: tuple[str, ...] = ("templateAdmins", "checklistAdmins")

    @property
    def display_name(self) -> str:
        return "infield configs"

    @classmethod
    def get_id(cls, item: APMConfig | APMConfigWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if not item.external_id:
            raise KeyError("APMConfig must have external_id")
        return item.external_id

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"externalId": id}

    @classmethod
    def get_required_capability(
        cls, items: collections.abc.Sequence[APMConfigWrite] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []

        actions = (
            [DataModelInstancesAcl.Action.Read]
            if read_only
            else [DataModelInstancesAcl.Action.Read, DataModelInstancesAcl.Action.Write]
        )

        return DataModelInstancesAcl(actions, DataModelInstancesAcl.Scope.SpaceID([APMConfig.space]))

    def create(self, items: APMConfigWriteList) -> NodeApplyResultList:
        result = self.client.data_modeling.instances.apply(
            nodes=items.as_nodes(), auto_create_direct_relations=True, replace=False
        )
        return result.nodes

    def retrieve(self, ids: SequenceNotStr[str]) -> APMConfigList:
        result = self.client.data_modeling.instances.retrieve(
            nodes=self._as_node_ids(ids), sources=APMConfig.view_id
        ).nodes
        return APMConfigList.from_nodes(result)

    def update(self, items: APMConfigWriteList) -> NodeApplyResultList:
        result = self.client.data_modeling.instances.apply(
            nodes=items.as_nodes(), auto_create_direct_relations=True, replace=True
        )
        return result.nodes

    def delete(self, ids: SequenceNotStr[str]) -> int:
        try:
            deleted = self.client.data_modeling.instances.delete(nodes=self._as_node_ids(ids))
        except CogniteAPIError as e:
            if "not exist" in e.message and "space" in e.message.lower():
                return 0
            raise e
        return len(deleted.nodes)

    @staticmethod
    def _as_node_ids(ids: SequenceNotStr[str]) -> list[NodeId]:
        return [NodeId(APMConfig.space, id) for id in ids]

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[APMConfig]:
        for node in iterate_instances(
            self.client, space=space, instance_type="node", source=APMConfig.view_id, console=self.console
        ):
            yield APMConfig.from_node(node)

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # Added by toolkit
        spec.add(
            ParameterSpec(
                ("featureConfiguration", "rootLocationConfiguration", "dataSetExternalId"),
                frozenset({"str"}),
                is_required=False,
                _is_nullable=False,
            )
        )
        spec.discard(
            ParameterSpec(
                (
                    "featureConfiguration",
                    "rootLocationConfiguration",
                    "dataSetId",
                ),
                frozenset({"int"}),
                is_required=False,
                _is_nullable=False,
            )
        )
        return spec

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        if isinstance(app_data_space_id := item.get("appDataSpaceId"), str):
            yield SpaceLoader, app_data_space_id
        if isinstance(customer_data_space_id := item.get("customerDataSpaceId"), str):
            yield SpaceLoader, customer_data_space_id
        for config in cls._get_root_location_configurations(item) or []:
            if isinstance(asset_external_id := config.get("assetExternalId"), str):
                yield AssetLoader, asset_external_id
            if isinstance(data_set_external_id := config.get("dataSetExternalId"), str):
                yield DataSetsLoader, data_set_external_id
            if isinstance(app_data_instance_space := config.get("appDataInstanceSpace"), str):
                yield SpaceLoader, app_data_instance_space
            if isinstance(source_data_instance_space := config.get("sourceDataInstanceSpace"), str):
                yield SpaceLoader, source_data_instance_space
            for key in cls._group_keys:
                for group in config.get(key, []):
                    if isinstance(group, str):
                        yield GroupResourceScopedLoader, group
            data_filters = config.get("dataFilters")
            if not isinstance(data_filters, dict):
                continue
            for key in cls._root_location_filters:
                filter_ = data_filters.get(key)
                if not isinstance(filter_, dict):
                    continue
                for data_set_external_id in filter_.get("dataSetExternalIds", []):
                    if isinstance(data_set_external_id, str):
                        yield DataSetsLoader, data_set_external_id
                for asset_external_id in filter_.get("assetSubtreeExternalIds", []):
                    if isinstance(asset_external_id, str):
                        yield AssetLoader, asset_external_id
                if app_data_instance_space := filter_.get("appDataInstanceSpace"):
                    if isinstance(app_data_instance_space, str):
                        yield SpaceLoader, app_data_instance_space

    def safe_read(self, filepath: Path | str) -> str:
        # The customerDataSpaceVersion is a string, but the user often writes it as an int.
        # YAML will then parse it as an int, for example, `3_0_2` will be parsed as `302`.
        # This is technically a user mistake, as you should quote the version in the YAML file.
        # However, we do not want to put this burden on the user (knowing the intricate workings of YAML),
        # so we fix it here.
        return quote_int_value_by_key_in_yaml(safe_read(filepath), key="customerDataSpaceVersion")

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> APMConfigWrite:
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
        return APMConfigWrite._load(resource)

    def dump_resource(self, resource: APMConfig, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_write().dump()
        local = local or {}
        if "existingVersion" not in local:
            # Existing version is typically not set when creating nodes, but we get it back
            # when we retrieve the node from the server.
            dumped.pop("existingVersion", None)

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
