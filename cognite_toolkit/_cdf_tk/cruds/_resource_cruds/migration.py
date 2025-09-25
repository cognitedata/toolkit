from collections.abc import Hashable, Iterable, Sequence, Sized
from functools import lru_cache
from typing import Any, final

from cognite.client.data_classes import capabilities
from cognite.client.data_classes.capabilities import Capability
from cognite.client.data_classes.data_modeling import NodeApplyList, NodeList, ViewId
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk._parameters import ANYTHING, ParameterSpec, ParameterSpecSet
from cognite_toolkit._cdf_tk.client.data_classes.migration import (
    ResourceViewMapping,
    ResourceViewMappingApply,
)
from cognite_toolkit._cdf_tk.constants import COGNITE_MIGRATION_SPACE
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceCRUD
from cognite_toolkit._cdf_tk.utils import in_dict

from .datamodel import SpaceCRUD, ViewCRUD


@final
class ResourceViewMappingCRUD(
    ResourceCRUD[str, ResourceViewMappingApply, ResourceViewMapping, NodeApplyList, NodeList[ResourceViewMapping]]
):
    folder_name = "migration"
    filename_pattern = (
        r"^.*\.ResourceViewMapping$"  # Matches all yaml files whose stem ends with '.ResourceViewMapping$'.
    )
    filetypes = frozenset({"yaml", "yml"})
    resource_cls = ResourceViewMapping
    resource_write_cls = ResourceViewMappingApply
    list_cls = NodeList[ResourceViewMapping]
    list_write_cls = NodeApplyList
    kind = "ResourceViewMapping"
    dependencies = frozenset({SpaceCRUD, ViewCRUD})
    _doc_url = "Instances/operation/applyNodeAndEdges"

    @property
    def display_name(self) -> str:
        return "resource view mapping"

    @classmethod
    def get_id(cls, item: ResourceViewMapping | ResourceViewMappingApply | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        return item.external_id

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"externalId": id}

    @classmethod
    def get_required_capability(
        cls, items: Sequence[ResourceViewMappingApply] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []

        actions = (
            [capabilities.DataModelInstancesAcl.Action.Read]
            if read_only
            else [capabilities.DataModelInstancesAcl.Action.Read, capabilities.DataModelInstancesAcl.Action.Write]
        )

        return capabilities.DataModelInstancesAcl(
            actions=actions, scope=capabilities.DataModelInstancesAcl.Scope.SpaceID([COGNITE_MIGRATION_SPACE])
        )

    def create(self, items: NodeApplyList) -> Sized:
        return self.client.migration.resource_view_mapping.upsert(items)

    def update(self, items: NodeApplyList) -> Sized:
        return self.client.migration.resource_view_mapping.upsert(items)

    def retrieve(self, ids: SequenceNotStr[str]) -> NodeList[ResourceViewMapping]:
        return self.client.migration.resource_view_mapping.retrieve(external_id=ids)

    def delete(self, ids: SequenceNotStr[str]) -> int:
        result = self.client.migration.resource_view_mapping.delete(external_id=ids)
        return len(result)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[ResourceViewMapping]:
        if space == COGNITE_MIGRATION_SPACE:
            return self.client.migration.resource_view_mapping.list(limit=-1)
        else:
            return []

    @classmethod
    def get_dependent_items(cls, item: dict) -> "Iterable[tuple[type[ResourceCRUD], Hashable]]":
        yield SpaceCRUD, COGNITE_MIGRATION_SPACE

        yield ViewCRUD, ResourceViewMapping.get_source()

        if "viewId" in item:
            view_id = item["viewId"]
            if isinstance(view_id, dict) and in_dict(("space", "externalId"), view_id):
                yield ViewCRUD, ViewId.load(view_id)

    def dump_resource(self, resource: ResourceViewMapping, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_write().dump(context="local")
        local = local or {}
        if "existingVersion" not in local:
            # Existing version is typically not set when creating nodes, but we get it back
            # when we retrieve the node from the server.
            dumped.pop("existingVersion", None)
        return dumped

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # Removed by the SDK
        spec.add(ParameterSpec(("instanceType",), frozenset({"str"}), is_required=False, _is_nullable=False))
        # Sources are used when writing to the API.
        spec.add(ParameterSpec(("sources",), frozenset({"list"}), is_required=False, _is_nullable=False))
        spec.add(ParameterSpec(("sources", ANYTHING), frozenset({"list"}), is_required=False, _is_nullable=False))
        # Space is hardcoded and thus not part of the spec.
        spec.add(ParameterSpec(("space",), frozenset({"str"}), is_required=False, _is_nullable=False))
        return spec
