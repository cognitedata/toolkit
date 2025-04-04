import collections.abc
from collections.abc import Hashable, Iterable
from functools import lru_cache
from typing import Any, final

from cognite.client.data_classes.capabilities import Capability
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk._parameters import ParameterSpec, ParameterSpecSet
from cognite_toolkit._cdf_tk.client.data_classes.apm_config_v1 import (
    APMConfig,
    APMConfigList,
    APMConfigWrite,
    APMConfigWriteList,
)
from cognite_toolkit._cdf_tk.loaders._base_loaders import ResourceLoader

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
        raise NotImplementedError

    def create(self, items: APMConfigWriteList) -> APMConfigList:
        raise NotImplementedError

    def retrieve(self, ids: SequenceNotStr[str]) -> APMConfigList:
        raise NotImplementedError

    def update(self, items: APMConfigWriteList) -> APMConfigList:
        raise NotImplementedError

    def delete(self, ids: SequenceNotStr[str | int]) -> int:
        raise NotImplementedError

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[APMConfig]:
        raise NotImplementedError

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
        raise NotImplementedError
