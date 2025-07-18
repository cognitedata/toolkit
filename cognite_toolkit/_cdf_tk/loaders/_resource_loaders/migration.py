from collections.abc import Hashable, Iterable, Sequence, Sized
from typing import Any, final

from cognite.client.data_classes import capabilities
from cognite.client.data_classes.capabilities import Capability
from cognite.client.data_classes.data_modeling import NodeApplyList, NodeList
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.data_classes.migration import (
    ViewSource,
    ViewSourceApply,
)
from cognite_toolkit._cdf_tk.constants import COGNITE_MIGRATION_SPACE
from cognite_toolkit._cdf_tk.loaders._base_loaders import ResourceLoader

from .datamodel_loaders import SpaceLoader, ViewLoader


@final
class ViewSourceLoader(ResourceLoader[str, ViewSourceApply, ViewSource, NodeApplyList, NodeList[ViewSource]]):
    folder_name = "migration"
    filename_pattern = r"^.*\.ViewSource$"  # Matches all yaml files whose stem ends with '.ViewSource'.
    filetypes = frozenset({"yaml", "yml"})
    resource_cls = ViewSource
    resource_write_cls = ViewSourceApply
    list_cls = NodeList[ViewSource]
    list_write_cls = NodeApplyList
    kind = "ViewSource"
    dependencies = frozenset({SpaceLoader, ViewLoader})
    _doc_url = "Instances/operation/applyNodeAndEdges"

    @property
    def display_name(self) -> str:
        return "view sources"

    @classmethod
    def get_id(cls, item: ViewSource | ViewSourceApply | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        return item.external_id

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"externalId": id}

    @classmethod
    def get_required_capability(
        cls, items: Sequence[ViewSourceApply] | None, read_only: bool
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
        return self.client.migration.view_source.upsert(items)

    def update(self, items: NodeApplyList) -> Sized:
        return self.client.migration.view_source.upsert(items)

    def retrieve(self, ids: SequenceNotStr[str]) -> NodeList[ViewSource]:
        return self.client.migration.view_source.retrieve(external_id=ids)

    def delete(self, ids: SequenceNotStr[str]) -> int:
        result = self.client.migration.view_source.delete(external_id=ids)
        return len(result)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[ViewSource]:
        if space == COGNITE_MIGRATION_SPACE:
            return self.client.migration.view_source.list(limit=-1)
        else:
            return []

    @classmethod
    def get_dependent_items(cls, item: dict) -> "Iterable[tuple[type[ResourceLoader], Hashable]]":
        yield SpaceLoader, COGNITE_MIGRATION_SPACE

        yield ViewLoader, ViewSource.get_source()
