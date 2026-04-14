from collections.abc import Hashable, Iterable, Sequence
from typing import Any, Literal, final

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.request_classes.filters import ClassicFilter
from cognite_toolkit._cdf_tk.client.resource_classes.group import (
    AclType,
    AllScope,
    DataSetScope,
    RelationshipsAcl,
    ScopeDefinition,
)
from cognite_toolkit._cdf_tk.client.resource_classes.relationship import RelationshipRequest, RelationshipResponse
from cognite_toolkit._cdf_tk.resource_ios._base_ios import ResourceIO
from cognite_toolkit._cdf_tk.utils.acl_helper import dataset_scoped_resource
from cognite_toolkit._cdf_tk.yaml_classes import RelationshipYAML

from .classic import AssetIO, EventIO, SequenceIO
from .data_organization import DataSetsIO, LabelIO
from .file import FileMetadataCRUD
from .timeseries import TimeSeriesCRUD


@final
class RelationshipIO(ResourceIO[ExternalId, RelationshipRequest, RelationshipResponse]):
    folder_name = "classic"
    resource_cls = RelationshipResponse
    resource_write_cls = RelationshipRequest
    kind = "Relationship"
    yaml_cls = RelationshipYAML
    dependencies = frozenset({DataSetsIO, AssetIO, EventIO, SequenceIO, FileMetadataCRUD, TimeSeriesCRUD, LabelIO})
    _doc_url = "Relationships/operation/createRelationships"

    @property
    def display_name(self) -> str:
        return "relationships"

    @classmethod
    def get_id(cls, item: RelationshipRequest | RelationshipResponse | dict) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        if not item.external_id:
            raise KeyError("Relationship must have external_id")
        return ExternalId(external_id=item.external_id)

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def get_minimum_scope(cls, items: Sequence[RelationshipRequest]) -> ScopeDefinition:
        return dataset_scoped_resource(items)

    @classmethod
    def create_acl(cls, actions: set[Literal["READ", "WRITE"]], scope: ScopeDefinition) -> Iterable[AclType]:
        if isinstance(scope, AllScope | DataSetScope):
            yield RelationshipsAcl(actions=sorted(actions), scope=scope)

    def create(self, items: Sequence[RelationshipRequest]) -> list[RelationshipResponse]:
        return self.client.tool.relationships.create(list(items))

    def retrieve(self, ids: Sequence[ExternalId]) -> list[RelationshipResponse]:
        return self.client.tool.relationships.retrieve(list(ids), ignore_unknown_ids=True)

    def update(self, items: Sequence[RelationshipRequest]) -> list[RelationshipResponse]:
        return self.client.tool.relationships.update(list(items))

    def delete(self, ids: Sequence[ExternalId]) -> int:
        if not ids:
            return 0
        try:
            self.client.tool.relationships.delete(list(ids))
        except ToolkitAPIError as e:
            if missing := {ExternalId.model_validate(item) for item in e.missing or []}:
                if existing := (set(ids) - missing):
                    self.client.tool.relationships.delete(list(existing))
                    return len(existing)
                else:
                    return 0
            raise
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[RelationshipResponse]:
        filter: ClassicFilter | None = None
        if data_set_external_id:
            filter = ClassicFilter(data_set_ids=[ExternalId(external_id=data_set_external_id)])
        for items in self.client.tool.relationships.iterate(filter=filter, limit=None):
            yield from items

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceIO], Hashable]]:
        """Returns all items that this item requires.

        For example, a TimeSeries requires a DataSet, so this method would return the
        DatasetLoader and identifier of that dataset.
        """
        if "dataSetExternalId" in item:
            yield DataSetsIO, ExternalId(external_id=item["dataSetExternalId"])
        for label in item.get("labels", []):
            if isinstance(label, dict):
                yield LabelIO, ExternalId(external_id=label["externalId"])
            elif isinstance(label, str):
                yield LabelIO, ExternalId(external_id=label)
        for connection in ["source", "target"]:
            type_key = f"{connection}Type"
            id_key = f"{connection}ExternalId"
            if type_key in item and id_key in item:
                type_value = item[type_key]
                id_value = item[id_key]
                if isinstance(id_value, str) and isinstance(type_value, str):
                    type_value = type_value.strip().casefold()
                    if type_value == "asset":
                        yield AssetIO, ExternalId(external_id=id_value)
                    elif type_value == "sequence":
                        yield SequenceIO, ExternalId(external_id=id_value)
                    elif type_value == "timeseries":
                        yield TimeSeriesCRUD, ExternalId(external_id=id_value)
                    elif type_value == "file":
                        yield FileMetadataCRUD, ExternalId(external_id=id_value)
                    elif type_value == "event":
                        yield EventIO, ExternalId(external_id=id_value)

    @classmethod
    def get_dependencies(cls, resource: RelationshipYAML) -> Iterable[tuple[type[ResourceIO], Identifier]]:
        if resource.data_set_external_id:
            yield DataSetsIO, ExternalId(external_id=resource.data_set_external_id)
        for label in resource.labels or []:
            yield LabelIO, ExternalId(external_id=label.externalId)
        type_to_crud: dict[str, type[ResourceIO]] = {
            "asset": AssetIO,
            "sequence": SequenceIO,
            "timeseries": TimeSeriesCRUD,
            "file": FileMetadataCRUD,
            "event": EventIO,
        }
        for type_value, id_value in [
            (resource.source_type, resource.source_external_id),
            (resource.target_type, resource.target_external_id),
        ]:
            crud = type_to_crud.get(type_value.strip().casefold())
            if crud:
                yield crud, ExternalId(external_id=id_value)

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> RelationshipRequest:
        if ds_external_id := resource.pop("dataSetExternalId", None):
            resource["dataSetId"] = self.client.lookup.data_sets.id(ds_external_id, is_dry_run)
        return RelationshipRequest.model_validate(resource)

    def dump_resource(self, resource: RelationshipResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_request_resource().dump()
        local = local or {}
        if data_set_id := dumped.pop("dataSetId", None):
            dumped["dataSetExternalId"] = self.client.lookup.data_sets.external_id(data_set_id)
        if not dumped.get("labels") and "labels" not in local:
            dumped.pop("labels", None)
        return dumped
