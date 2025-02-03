from collections.abc import Hashable, Iterable, Sequence
from functools import lru_cache
from typing import Any, final

from cognite.client.data_classes import (
    Relationship,
    RelationshipList,
    RelationshipWrite,
    RelationshipWriteList,
    capabilities,
)
from cognite.client.data_classes.capabilities import Capability
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk._parameters import ANY_INT, ParameterSpec, ParameterSpecSet
from cognite_toolkit._cdf_tk.loaders._base_loaders import ResourceLoader

from .classic_loaders import AssetLoader, EventLoader, SequenceLoader
from .data_organization_loaders import DataSetsLoader, LabelLoader
from .file_loader import FileMetadataLoader
from .timeseries_loaders import TimeSeriesLoader


@final
class RelationshipLoader(ResourceLoader[str, RelationshipWrite, Relationship, RelationshipWriteList, RelationshipList]):
    folder_name = "classic"
    filename_pattern = r"^.*\.Relationship$"  # Matches all yaml files whose stem ends with '.Asset'.
    filetypes = frozenset({"yaml", "yml"})
    resource_cls = Relationship
    resource_write_cls = RelationshipWrite
    list_cls = RelationshipList
    list_write_cls = RelationshipWriteList
    kind = "Relationship"
    dependencies = frozenset(
        {DataSetsLoader, AssetLoader, EventLoader, SequenceLoader, FileMetadataLoader, TimeSeriesLoader, LabelLoader}
    )
    _doc_url = "Relationships/operation/createRelationships"

    @property
    def display_name(self) -> str:
        return "relationships"

    @classmethod
    def get_id(cls, item: Relationship | RelationshipWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if not item.external_id:
            raise KeyError("Relationship must have external_id")
        return item.external_id

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"externalId": id}

    @classmethod
    def get_required_capability(
        cls, items: Sequence[RelationshipWrite] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []
        scope: capabilities.RelationshipsAcl.Scope.All | capabilities.RelationshipsAcl.Scope.DataSet = (  # type: ignore[valid-type]
            capabilities.RelationshipsAcl.Scope.All()
        )

        actions = (
            [capabilities.RelationshipsAcl.Action.Read]
            if read_only
            else [capabilities.RelationshipsAcl.Action.Read, capabilities.RelationshipsAcl.Action.Write]
        )

        if items:
            if data_set_ids := {item.data_set_id for item in items if item.data_set_id}:
                scope = capabilities.RelationshipsAcl.Scope.DataSet(list(data_set_ids))

        return capabilities.RelationshipsAcl(
            actions,
            scope,  # type: ignore[arg-type]
        )

    def create(self, items: RelationshipWriteList) -> RelationshipList:
        return self.client.relationships.create(items)

    def retrieve(self, ids: SequenceNotStr[str]) -> RelationshipList:
        return self.client.relationships.retrieve_multiple(external_ids=ids, ignore_unknown_ids=True)

    def update(self, items: RelationshipWriteList) -> RelationshipList:
        return self.client.relationships.update(items)

    def delete(self, ids: SequenceNotStr[str]) -> int:
        try:
            self.client.relationships.delete(external_id=ids)
        except (CogniteAPIError, CogniteNotFoundError) as e:
            non_existing = set(e.failed or [])
            if existing := [id_ for id_ in ids if id_ not in non_existing]:
                self.client.relationships.delete(external_id=existing)
            return len(existing)
        else:
            return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[Relationship]:
        return iter(
            self.client.relationships(data_set_external_ids=[data_set_external_id] if data_set_external_id else None)
        )

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # Added by toolkit
        spec.add(ParameterSpec(("dataSetExternalId",), frozenset({"str"}), is_required=False, _is_nullable=False))
        spec.discard(ParameterSpec(("dataSetId",), frozenset({"int"}), is_required=False, _is_nullable=False))

        # Failure from generation of spec
        spec.add(
            ParameterSpec(("labels", ANY_INT, "externalId"), frozenset({"str"}), is_required=False, _is_nullable=False)
        )
        return spec

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        """Returns all items that this item requires.

        For example, a TimeSeries requires a DataSet, so this method would return the
        DatasetLoader and identifier of that dataset.
        """
        if "dataSetExternalId" in item:
            yield DataSetsLoader, item["dataSetExternalId"]
        for label in item.get("labels", []):
            if isinstance(label, dict):
                yield LabelLoader, label["externalId"]
            elif isinstance(label, str):
                yield LabelLoader, label
        for connection in ["source", "target"]:
            type_key = f"{connection}Type"
            id_key = f"{connection}ExternalId"
            if type_key in item and id_key in item:
                type_value = item[type_key]
                id_value = item[id_key]
                if isinstance(id_value, str) and isinstance(type_value, str):
                    type_value = type_value.strip().casefold()
                    if type_value == "asset":
                        yield AssetLoader, id_value
                    elif type_value == "sequence":
                        yield SequenceLoader, id_value
                    elif type_value == "timeseries":
                        yield TimeSeriesLoader, id_value
                    elif type_value == "file":
                        yield FileMetadataLoader, id_value
                    elif type_value == "event":
                        yield EventLoader, id_value

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> RelationshipWrite:
        if ds_external_id := resource.pop("dataSetExternalId", None):
            resource["dataSetId"] = self.client.lookup.data_sets.id(ds_external_id, is_dry_run)
        return RelationshipWrite._load(resource)

    def dump_resource(self, resource: Relationship, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_write().dump()
        local = local or {}
        if data_set_id := dumped.pop("dataSetId", None):
            dumped["dataSetExternalId"] = self.client.lookup.data_sets.external_id(data_set_id)
        if not dumped.get("labels") and "labels" not in local:
            dumped.pop("labels", None)
        return dumped
