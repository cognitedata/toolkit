from collections.abc import Hashable, Iterable, Sequence
from typing import Any, final

from cognite.client.data_classes import capabilities
from cognite.client.data_classes.capabilities import Capability

from cognite_toolkit._cdf_tk.client.identifiers import (
    ExternalId,
    InternalId,
    NameId,
    ThreeDModelRevisionId,
)
from cognite_toolkit._cdf_tk.client.resource_classes.three_d import (
    ThreeDModelClassicRequest,
    ThreeDModelClassicResponse,
)
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceContainerCRUD, ResourceCRUD
from cognite_toolkit._cdf_tk.resource_classes import ThreeDModelYAML
from cognite_toolkit._cdf_tk.utils import sanitize_filename

from .data_organization import DataSetsCRUD


@final
class ThreeDModelCRUD(ResourceContainerCRUD[NameId, ThreeDModelClassicRequest, ThreeDModelClassicResponse]):
    folder_name = "3dmodels"
    resource_cls = ThreeDModelClassicResponse
    resource_write_cls = ThreeDModelClassicRequest
    kind = "3DModel"
    yaml_cls = ThreeDModelYAML
    dependencies = frozenset({DataSetsCRUD})
    _doc_url = "3D-Models/operation/create3DModels"
    item_name = "revisions"

    @property
    def display_name(self) -> str:
        return "3D models"

    @classmethod
    def get_id(cls, item: ThreeDModelClassicRequest | ThreeDModelClassicResponse | dict) -> NameId:
        if isinstance(item, dict):
            return NameId(name=item["name"])
        if not item.name:
            raise KeyError("3DModel must have name")
        return NameId(name=item.name)

    @classmethod
    def get_internal_id(cls, item: ThreeDModelClassicResponse | dict) -> int:
        if isinstance(item, dict):
            return item["id"]
        if not item.id:
            raise KeyError("3DModel must have id")
        return item.id

    @classmethod
    def dump_id(cls, id: NameId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def as_str(cls, id: NameId) -> str:
        return sanitize_filename(id.name)

    @classmethod
    def get_required_capability(
        cls, items: Sequence[ThreeDModelClassicRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []
        scope: capabilities.ThreeDAcl.Scope.All | capabilities.ThreeDAcl.Scope.DataSet = (  # type: ignore[valid-type]
            capabilities.ThreeDAcl.Scope.All()
        )
        if items:
            if data_set_ids := {item.data_set_id for item in items or [] if item.data_set_id}:
                scope = capabilities.ThreeDAcl.Scope.DataSet(list(data_set_ids))

        actions = (
            [capabilities.ThreeDAcl.Action.Read]
            if read_only
            else [
                capabilities.ThreeDAcl.Action.Read,
                capabilities.ThreeDAcl.Action.Create,
                capabilities.ThreeDAcl.Action.Update,
                capabilities.ThreeDAcl.Action.Delete,
            ]
        )

        return capabilities.ThreeDAcl(actions, scope)

    def create(self, items: Sequence[ThreeDModelClassicRequest]) -> list[ThreeDModelClassicResponse]:
        return self.client.tool.three_d.models_classic.create(items)

    def retrieve(self, ids: Sequence[NameId]) -> list[ThreeDModelClassicResponse]:
        selected_names = {id_.name for id_ in ids}
        output: list[ThreeDModelClassicResponse] = []
        for models in self.client.tool.three_d.models_classic.iterate(limit=None):
            for model in models:
                if model.name in selected_names:
                    output.append(model)
                    selected_names.discard(model.name)
            if not selected_names:
                break
        return output

    def update(self, items: Sequence[ThreeDModelClassicRequest]) -> list[ThreeDModelClassicResponse]:
        return self.client.tool.three_d.models_classic.update(items)

    def delete(self, ids: Sequence[NameId]) -> int:
        models = self.retrieve(ids)
        internal_ids = [InternalId(id=model.id) for model in models]
        self.client.tool.three_d.models_classic.delete(internal_ids)
        return len(models)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[ThreeDModelClassicResponse]:
        # DataSet filtering is not supported by the API, so we filter client-side.
        data_set_id = self.client.lookup.data_sets.id(data_set_external_id) if data_set_external_id else None
        for models in self.client.tool.three_d.models_classic.iterate(limit=None):
            if data_set_id is None:
                yield from models
                continue
            for model in models:
                if model.data_set_id == data_set_id:
                    yield model

    def drop_data(self, ids: Sequence[NameId]) -> int:
        models = self.retrieve(ids)
        count = 0
        for model in models:
            for revisions in self.client.tool.three_d.revisions_classic.iterate(model_id=model.id, limit=None):
                revision_ids = [ThreeDModelRevisionId(id=r.id, model_id=r.model_id) for r in revisions]
                self.client.tool.three_d.revisions_classic.delete(ids=revision_ids)
                count += len(revisions)
        return count

    def count(self, ids: Sequence[NameId]) -> int:
        models = self.retrieve(ids)
        count = 0
        for model in models:
            for revisions in self.client.tool.three_d.revisions_classic.iterate(model_id=model.id, limit=None):
                count += len(revisions)
        return count

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceCRUD], Hashable]]:
        """Returns all items that this item requires.

        For example, a TimeSeries requires a DataSet, so this method would return the
        DatasetLoader and identifier of that dataset.
        """
        if "dataSetExternalId" in item:
            yield DataSetsCRUD, ExternalId(external_id=item["dataSetExternalId"])

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> ThreeDModelClassicRequest:
        if ds_external_id := resource.pop("dataSetExternalId", None):
            resource["dataSetId"] = self.client.lookup.data_sets.id(ds_external_id, is_dry_run)
        return ThreeDModelClassicRequest.model_validate(resource)

    def dump_resource(
        self, resource: ThreeDModelClassicResponse, local: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        dumped = resource.as_request_resource().dump()
        local = local or {}
        if data_set_id := dumped.pop("dataSetId", None):
            dumped["dataSetExternalId"] = self.client.lookup.data_sets.external_id(data_set_id)
        if not dumped.get("metadata") and "metadata" in local:
            # Remove empty metadata {}.
            dumped.pop("metadata", None)
        return dumped
