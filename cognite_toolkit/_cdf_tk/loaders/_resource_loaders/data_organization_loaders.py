# Copyright 2023 Cognite AS
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import annotations

import json
from collections.abc import Hashable, Iterable, Sequence
from functools import lru_cache
from pathlib import Path
from typing import Any, cast, final

from cognite.client.data_classes import (
    DataSet,
    DataSetList,
    DataSetWrite,
    DataSetWriteList,
    LabelDefinition,
    LabelDefinitionList,
    LabelDefinitionWrite,
    capabilities,
)
from cognite.client.data_classes._base import T_CogniteResourceList
from cognite.client.data_classes.capabilities import (
    Capability,
    DataSetsAcl,
)
from cognite.client.data_classes.labels import LabelDefinitionWriteList
from cognite.client.exceptions import CogniteAPIError, CogniteDuplicatedError, CogniteNotFoundError
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk._parameters import ANY_STR, ANYTHING, ParameterSpec, ParameterSpecSet
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitRequiredValueError,
)
from cognite_toolkit._cdf_tk.loaders._base_loaders import ResourceLoader
from cognite_toolkit._cdf_tk.utils import (
    CDFToolConfig,
    load_yaml_inject_variables,
)

from .auth_loaders import GroupAllScopedLoader


@final
class DataSetsLoader(ResourceLoader[str, DataSetWrite, DataSet, DataSetWriteList, DataSetList]):
    support_drop = False
    folder_name = "data_sets"
    resource_cls = DataSet
    resource_write_cls = DataSetWrite
    list_cls = DataSetList
    list_write_cls = DataSetWriteList
    kind = "DataSet"
    dependencies = frozenset({GroupAllScopedLoader})
    _doc_url = "Data-sets/operation/createDataSets"

    @classmethod
    def get_required_capability(cls, items: DataSetWriteList | None) -> Capability | list[Capability]:
        if not items and items is not None:
            return []
        return DataSetsAcl(
            [DataSetsAcl.Action.Read, DataSetsAcl.Action.Write, DataSetsAcl.Action.Owner],
            DataSetsAcl.Scope.All(),
        )

    @classmethod
    def get_id(cls, item: DataSet | DataSetWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if item.external_id is None:
            raise ToolkitRequiredValueError("DataSet must have external_id set.")
        return item.external_id

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"externalId": id}

    def load_resource(self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool) -> DataSetWriteList:
        resource = load_yaml_inject_variables(filepath, {})

        data_sets = [resource] if isinstance(resource, dict) else resource

        for data_set in data_sets:
            if data_set.get("metadata"):
                for key, value in data_set["metadata"].items():
                    data_set["metadata"][key] = json.dumps(value)
            if data_set.get("writeProtected") is None:
                # Todo: Setting missing default value, bug in SDK.
                data_set["writeProtected"] = False
            if data_set.get("metadata") is None:
                # Todo: Wrongly set to empty dict, bug in SDK.
                data_set["metadata"] = {}

        return DataSetWriteList.load(data_sets)

    def create(self, items: Sequence[DataSetWrite]) -> DataSetList:
        items = list(items)
        created = DataSetList([], cognite_client=self.client)
        # There is a bug in the data set API, so only one duplicated data set is returned at the time,
        # so we need to iterate.
        while len(items) > 0:
            try:
                created.extend(DataSetList(self.client.data_sets.create(items)))
                return created
            except CogniteDuplicatedError as e:
                if len(e.duplicated) < len(items):
                    for dup in e.duplicated:
                        ext_id = dup.get("externalId", None)
                        for item in items:
                            if item.external_id == ext_id:
                                items.remove(item)
                else:
                    items = []
        return created

    def retrieve(self, ids: SequenceNotStr[str]) -> DataSetList:
        return self.client.data_sets.retrieve_multiple(
            external_ids=cast(SequenceNotStr[str], ids), ignore_unknown_ids=True
        )

    def update(self, items: DataSetWriteList) -> DataSetList:
        return self.client.data_sets.update(items, mode="replace")

    def delete(self, ids: SequenceNotStr[str]) -> int:
        raise NotImplementedError("CDF does not support deleting data sets.")

    def iterate(self) -> Iterable[DataSet]:
        return iter(self.client.data_sets)

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # Added by toolkit, toolkit will automatically convert metadata to json
        spec.add(
            ParameterSpec(
                ("metadata", ANY_STR, ANYTHING), frozenset({"unknown"}), is_required=False, _is_nullable=False
            )
        )
        return spec


@final
class LabelLoader(
    ResourceLoader[str, LabelDefinitionWrite, LabelDefinition, LabelDefinitionWriteList, LabelDefinitionList]
):
    folder_name = "classic"
    filename_pattern = r"^.*Label$"  # Matches all yaml files whose stem ends with *Label.
    resource_cls = LabelDefinition
    resource_write_cls = LabelDefinitionWrite
    list_cls = LabelDefinitionList
    list_write_cls = LabelDefinitionWriteList
    kind = "Label"
    dependencies = frozenset({DataSetsLoader, GroupAllScopedLoader})
    _doc_url = "Labels/operation/createLabelDefinitions"

    @property
    def display_name(self) -> str:
        return self.kind

    @classmethod
    def get_id(cls, item: LabelDefinition | LabelDefinitionWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if not item.external_id:
            raise ToolkitRequiredValueError("LabelDefinition must have external_id set.")
        return item.external_id

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"externalId": id}

    @classmethod
    def get_required_capability(cls, items: LabelDefinitionWriteList | None) -> Capability | list[Capability]:
        if not items and items is not None:
            return []
        scope: capabilities.LabelsAcl.Scope.All | capabilities.LabelsAcl.Scope.DataSet = (  # type: ignore[valid-type]
            capabilities.LabelsAcl.Scope.All()
        )
        if items:
            if data_set_ids := {item.data_set_id for item in items if item.data_set_id}:
                scope = capabilities.LabelsAcl.Scope.DataSet(list(data_set_ids))

        return capabilities.LabelsAcl(
            [capabilities.LabelsAcl.Action.Read, capabilities.LabelsAcl.Action.Write],
            scope,  # type: ignore[arg-type]
        )

    def create(self, items: LabelDefinitionWriteList) -> LabelDefinitionList:
        return self.client.labels.create(items)

    def retrieve(self, ids: SequenceNotStr[str]) -> LabelDefinitionList:
        return self.client.labels.retrieve(ids, ignore_unknown_ids=True)

    def update(self, items: T_CogniteResourceList) -> LabelDefinitionList:
        existing = self.client.labels.retrieve([item.external_id for item in items])
        if existing:
            self.delete([item.external_id for item in items])
        return self.client.labels.create(items)

    def delete(self, ids: SequenceNotStr[str]) -> int:
        try:
            self.client.labels.delete(ids)
        except (CogniteAPIError, CogniteNotFoundError) as e:
            non_existing = set(e.failed or [])
            if existing := [id_ for id_ in ids if id_ not in non_existing]:
                self.client.labels.delete(existing)
            return len(existing)
        else:
            # All deleted successfully
            return len(ids)

    def iterate(self) -> Iterable[LabelDefinition]:
        return iter(self.client.labels)

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # Added by toolkit
        spec.add(ParameterSpec(("dataSetExternalId",), frozenset({"str"}), is_required=False, _is_nullable=False))
        return spec

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        """Returns all items that this item requires.

        For example, a TimeSeries requires a DataSet, so this method would return the
        DatasetLoader and identifier of that dataset.
        """
        if "dataSetExternalId" in item:
            yield DataSetsLoader, item["dataSetExternalId"]

    def load_resource(
        self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> LabelDefinitionWrite | LabelDefinitionWriteList | None:
        raw_yaml = load_yaml_inject_variables(filepath, ToolGlobals.environment_variables())
        items: list[dict[str, Any]] = [raw_yaml] if isinstance(raw_yaml, dict) else raw_yaml
        for item in items:
            if "dataSetExternalId" in item:
                ds_external_id = item.pop("dataSetExternalId")
                item["dataSetId"] = ToolGlobals.verify_dataset(
                    ds_external_id,
                    skip_validation=skip_validation,
                    action="replace dataSetExternalId with dataSetId in label",
                )
        loaded = LabelDefinitionWriteList.load(items)
        return loaded[0] if isinstance(raw_yaml, dict) else loaded

    def _are_equal(
        self, local: LabelDefinitionWrite, cdf_resource: LabelDefinition, return_dumped: bool = False
    ) -> bool | tuple[bool, dict[str, Any], dict[str, Any]]:
        local_dumped = local.dump()
        cdf_dumped = cdf_resource.as_write().dump()
        if local_dumped.get("dataSetId") == -1 and "dataSetId" in cdf_dumped:
            # Dry run
            local_dumped["dataSetId"] = cdf_dumped["dataSetId"]
        return self._return_are_equal(local_dumped, cdf_dumped, return_dumped)
