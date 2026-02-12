from collections.abc import Hashable, Iterable, Sequence
from typing import Any, final

from cognite.client.data_classes import capabilities
from cognite.client.data_classes.capabilities import Capability
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.request_classes.filters import ClassicFilter
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.relationship import RelationshipRequest, RelationshipResponse
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceCRUD
from cognite_toolkit._cdf_tk.resource_classes import RelationshipYAML

from .classic import AssetCRUD, EventCRUD, SequenceCRUD
from .data_organization import DataSetsCRUD, LabelCRUD
from .file import FileMetadataCRUD
from .timeseries import TimeSeriesCRUD


@final
class RelationshipCRUD(ResourceCRUD[ExternalId, RelationshipRequest, RelationshipResponse]):
    folder_name = "classic"
    resource_cls = RelationshipResponse
    resource_write_cls = RelationshipRequest
    kind = "Relationship"
    yaml_cls = RelationshipYAML
    dependencies = frozenset(
        {DataSetsCRUD, AssetCRUD, EventCRUD, SequenceCRUD, FileMetadataCRUD, TimeSeriesCRUD, LabelCRUD}
    )
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
    def get_required_capability(
        cls, items: Sequence[RelationshipRequest] | None, read_only: bool
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

        return capabilities.RelationshipsAcl(actions, scope)

    def create(self, items: Sequence[RelationshipRequest]) -> list[RelationshipResponse]:
        return self.client.tool.relationships.create(list(items))

    def retrieve(self, ids: SequenceNotStr[ExternalId]) -> list[RelationshipResponse]:
        return self.client.tool.relationships.retrieve(list(ids), ignore_unknown_ids=True)

    def update(self, items: Sequence[RelationshipRequest]) -> list[RelationshipResponse]:
        return self.client.tool.relationships.update(list(items))

    def delete(self, ids: SequenceNotStr[ExternalId]) -> int:
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
        for items in self.client.tool.relationships.iterate(filter=filter):
            yield from items

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceCRUD], Hashable]]:
        """Returns all items that this item requires.

        For example, a TimeSeries requires a DataSet, so this method would return the
        DatasetLoader and identifier of that dataset.
        """
        if "dataSetExternalId" in item:
            yield DataSetsCRUD, item["dataSetExternalId"]
        for label in item.get("labels", []):
            if isinstance(label, dict):
                yield LabelCRUD, ExternalId(external_id=label["externalId"])
            elif isinstance(label, str):
                yield LabelCRUD, ExternalId(external_id=label)
        for connection in ["source", "target"]:
            type_key = f"{connection}Type"
            id_key = f"{connection}ExternalId"
            if type_key in item and id_key in item:
                type_value = item[type_key]
                id_value = item[id_key]
                if isinstance(id_value, str) and isinstance(type_value, str):
                    type_value = type_value.strip().casefold()
                    if type_value == "asset":
                        yield AssetCRUD, ExternalId(external_id=id_value)
                    elif type_value == "sequence":
                        yield SequenceCRUD, ExternalId(external_id=id_value)
                    elif type_value == "timeseries":
                        yield TimeSeriesCRUD, ExternalId(external_id=id_value)
                    elif type_value == "file":
                        yield FileMetadataCRUD, ExternalId(external_id=id_value)
                    elif type_value == "event":
                        yield EventCRUD, ExternalId(external_id=id_value)

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
