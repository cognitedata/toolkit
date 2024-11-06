from __future__ import annotations

from collections.abc import Hashable, Iterable
from functools import lru_cache
from pathlib import Path
from typing import Any, final

from cognite.client.data_classes import (
    ThreeDModel,
    ThreeDModelList,
    ThreeDModelUpdate,
    ThreeDModelWrite,
    ThreeDModelWriteList,
    capabilities,
)
from cognite.client.data_classes.capabilities import Capability
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk._parameters import ParameterSpec, ParameterSpecSet
from cognite_toolkit._cdf_tk.loaders._base_loaders import ResourceContainerLoader, ResourceLoader
from cognite_toolkit._cdf_tk.utils import CDFToolConfig, load_yaml_inject_variables

from .data_organization_loaders import DataSetsLoader


@final
class ThreeDModelLoader(
    ResourceContainerLoader[str, ThreeDModelWrite, ThreeDModel, ThreeDModelWriteList, ThreeDModelList]
):
    folder_name = "3dmodels"
    filename_pattern = r"^.*\.3DModel$"  # Matches all yaml files whose stem ends with '.3DModel'.
    resource_cls = ThreeDModel
    resource_write_cls = ThreeDModelWrite
    list_cls = ThreeDModelList
    list_write_cls = ThreeDModelWriteList
    kind = "3DModel"
    dependencies = frozenset({DataSetsLoader})
    _doc_url = "3D-Models/operation/create3DModels"
    item_name = "revisions"

    @classmethod
    def get_id(cls, item: ThreeDModel | ThreeDModelWrite | dict) -> str:
        if isinstance(item, dict):
            return item["name"]
        if not item.name:
            raise KeyError("3DModel must have name")
        return item.name

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"name": id}

    @classmethod
    def get_required_capability(
        cls, items: ThreeDModelWriteList | None, read_only: bool
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

        return capabilities.ThreeDAcl(
            actions,
            scope,  # type: ignore[arg-type]
        )

    def create(self, items: ThreeDModelWriteList) -> ThreeDModelList:
        created = ThreeDModelList([])
        for item in items:
            new_item = self.client.three_d.models.create(**item.dump(camel_case=False))
            created.append(new_item)
        return created

    def retrieve(self, ids: SequenceNotStr[str]) -> ThreeDModelList:
        output = ThreeDModelList([])
        to_find = set(ids)
        for model in self.client.three_d.models:
            if model.name in to_find:
                output.append(model)
                to_find.remove(model.name)
                if not to_find:
                    break
        return output

    def update(self, items: ThreeDModelWriteList) -> ThreeDModelList:
        found = self.retrieve([item.name for item in items])
        id_by_name = {model.name: model.id for model in found}
        # 3D Model does not have an external identifier, only internal.
        # Thus, we cannot use the ThreeDModelWrite object to update the model,
        # instead we convert it to a ThreeDModelUpdate object.
        updates = []
        for item in items:
            if id_ := id_by_name.get(item.name):
                update = ThreeDModelUpdate(id=id_)
                if item.metadata:
                    update.metadata.set(item.metadata)
                if item.data_set_id:
                    update.data_set_id.set(item.data_set_id)
                # We cannot change the name of a 3D model as we use it as the identifier
                # Note this is expected
                updates.append(update)
        return self.client.three_d.models.update(updates, mode="replace")

    def delete(self, ids: SequenceNotStr[str]) -> int:
        models = self.retrieve(ids)
        self.client.three_d.models.delete(models.as_ids())
        return len(models)

    def iterate(self) -> Iterable[ThreeDModel]:
        return iter(self.client.three_d.models)

    def drop_data(self, ids: SequenceNotStr[str]) -> int:
        models = self.retrieve(ids)
        count = 0
        for model in models:
            revisions = self.client.three_d.revisions.list(model_id=model.id)
            self.client.three_d.revisions.delete(model_id=model.id, id=revisions.as_ids())
            count += len(revisions)
        return count

    def count(self, ids: SequenceNotStr[str]) -> int:
        models = self.retrieve(ids)
        count = 0
        for model in models:
            revisions = self.client.three_d.revisions.list(model_id=model.id)
            count += len(revisions)
        return count

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # Added by toolkit
        spec.add(ParameterSpec(("dataSetExternalId",), frozenset({"str"}), is_required=False, _is_nullable=False))

        # Should not be used, used for dataSetExternalId instead
        spec.discard(ParameterSpec(("dataSetId",), frozenset({"int"}), is_required=False, _is_nullable=False))
        return spec

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        """Returns all items that this item requires.

        For example, a TimeSeries requires a DataSet, so this method would return the
        DatasetLoader and identifier of that dataset.
        """
        if "dataSetExternalId" in item:
            yield DataSetsLoader, item["dataSetExternalId"]

    def load_resource(self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool) -> ThreeDModelWriteList:
        use_environment_variables = (
            ToolGlobals.environment_variables() if self.do_environment_variable_injection else {}
        )
        raw_yaml = load_yaml_inject_variables(filepath, use_environment_variables)

        resources = raw_yaml if isinstance(raw_yaml, list) else [raw_yaml]

        for resource in resources:
            if resource.get("dataSetExternalId") is not None:
                ds_external_id = resource.pop("dataSetExternalId")
                resource["dataSetId"] = ToolGlobals.verify_dataset(
                    ds_external_id, skip_validation, action="replace dataSetExternalId with dataSetId in 3D Model"
                )
        return ThreeDModelWriteList.load(resources)

    def _are_equal(
        self, local: ThreeDModelWrite, cdf_resource: ThreeDModel, return_dumped: bool = False
    ) -> bool | tuple[bool, dict[str, Any], dict[str, Any]]:
        local_dumped = local.dump()
        cdf_dumped = cdf_resource.as_write().dump()
        # Dry run
        if local_dumped.get("dataSetId") == -1 and "dataSetId" in cdf_dumped:
            local_dumped["dataSetId"] = cdf_dumped["dataSetId"]
        if not cdf_dumped.get("metadata") and not local_dumped.get("metadata"):
            cdf_dumped["metadata"] = local_dumped["metadata"] = {}
        return self._return_are_equal(local_dumped, cdf_dumped, return_dumped)
