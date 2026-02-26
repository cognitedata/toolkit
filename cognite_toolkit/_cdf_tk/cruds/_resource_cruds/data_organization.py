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


import json
from collections.abc import Hashable, Iterable, Sequence
from typing import Any, final

from cognite.client.data_classes import capabilities
from cognite.client.data_classes.capabilities import Capability, DataSetsAcl

from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.request_classes.filters import ClassicFilter
from cognite_toolkit._cdf_tk.client.resource_classes.dataset import DataSetRequest, DataSetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.label import LabelRequest, LabelResponse
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceCRUD
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitRequiredValueError,
)
from cognite_toolkit._cdf_tk.resource_classes import DataSetYAML, LabelsYAML
from cognite_toolkit._cdf_tk.utils.file import sanitize_filename

from .auth import GroupAllScopedCRUD


@final
class DataSetsCRUD(ResourceCRUD[ExternalId, DataSetRequest, DataSetResponse]):
    support_drop = False
    folder_name = "data_sets"
    resource_cls = DataSetResponse
    resource_write_cls = DataSetRequest
    yaml_cls = DataSetYAML
    kind = "DataSet"
    dependencies = frozenset({GroupAllScopedCRUD})
    _doc_url = "Data-sets/operation/createDataSets"

    @property
    def display_name(self) -> str:
        return "data sets"

    @classmethod
    def get_required_capability(
        cls, items: Sequence[DataSetRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []

        actions = (
            [DataSetsAcl.Action.Read]
            if read_only
            else [DataSetsAcl.Action.Read, DataSetsAcl.Action.Write, DataSetsAcl.Action.Owner]
        )

        return DataSetsAcl(
            actions,
            DataSetsAcl.Scope.All(),
        )

    @classmethod
    def get_id(cls, item: DataSetRequest | DataSetResponse | dict) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        if not item.external_id:
            raise ToolkitRequiredValueError("DataSet must have external_id set.")
        return item.as_id()

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def as_str(cls, id: ExternalId) -> str:
        return sanitize_filename(id.external_id)

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> DataSetRequest:
        if resource.get("metadata"):
            for key, value in list(resource["metadata"].items()):
                if isinstance(value, dict | list):
                    resource["metadata"][key] = json.dumps(value)
        return DataSetRequest._load(resource)

    def dump_resource(self, resource: DataSetResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_write().dump()
        local = local or {}
        if "writeProtected" not in local and dumped.get("writeProtected") is False:
            # Default value is False, so we don't need to dump it.
            dumped.pop("writeProtected")
        if "metadata" not in local and not dumped.get("metadata"):
            # Default value is empty dict, so we don't need to dump it.
            dumped.pop("metadata", None)
        if "metadata" in dumped and "metadata" in local:
            meta_local = local["metadata"]
            for key, value in list(dumped["metadata"].items()):
                if isinstance(meta_local.get(key), dict | list):
                    try:
                        converted = json.loads(value)
                    except json.JSONDecodeError:
                        continue
                    dumped["metadata"][key] = converted

        return dumped

    def create(self, items: Sequence[DataSetRequest]) -> list[DataSetResponse]:
        return self.client.tool.datasets.create(list(items))

    def retrieve(self, ids: Sequence[ExternalId]) -> list[DataSetResponse]:
        return self.client.tool.datasets.retrieve(list(ids), ignore_unknown_ids=True)

    def update(self, items: Sequence[DataSetRequest]) -> list[DataSetResponse]:
        return self.client.tool.datasets.update(list(items), mode="replace")

    def delete(self, ids: Sequence[ExternalId]) -> int:
        raise NotImplementedError("CDF does not support deleting data sets.")

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[DataSetResponse]:
        for items in self.client.tool.datasets.iterate(limit=None):
            yield from items


@final
class LabelCRUD(ResourceCRUD[ExternalId, LabelRequest, LabelResponse]):
    folder_name = "classic"
    resource_cls = LabelResponse
    resource_write_cls = LabelRequest
    yaml_cls = LabelsYAML
    kind = "Label"
    dependencies = frozenset({DataSetsCRUD, GroupAllScopedCRUD})
    _doc_url = "Labels/operation/createLabelDefinitions"
    support_update = False

    @property
    def display_name(self) -> str:
        return "labels"

    @classmethod
    def get_id(cls, item: LabelRequest | LabelResponse | dict) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        if not item.external_id:
            raise ToolkitRequiredValueError("Label must have external_id set.")
        return item.as_id()

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def get_required_capability(
        cls, items: Sequence[LabelRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []
        scope: capabilities.LabelsAcl.Scope.All | capabilities.LabelsAcl.Scope.DataSet = (  # type: ignore[valid-type]
            capabilities.LabelsAcl.Scope.All()
        )
        if items:
            if data_set_ids := {item.data_set_id for item in items if item.data_set_id}:
                scope = capabilities.LabelsAcl.Scope.DataSet(list(data_set_ids))

        actions = (
            [capabilities.LabelsAcl.Action.Read]
            if read_only
            else [capabilities.LabelsAcl.Action.Read, capabilities.LabelsAcl.Action.Write]
        )

        return capabilities.LabelsAcl(actions, scope)

    def create(self, items: Sequence[LabelRequest]) -> list[LabelResponse]:
        return self.client.tool.labels.create(list(items))

    def retrieve(self, ids: Sequence[ExternalId]) -> list[LabelResponse]:
        return self.client.tool.labels.retrieve(list(ids), ignore_unknown_ids=True)

    def delete(self, ids: Sequence[ExternalId]) -> int:
        if not ids:
            return 0
        try:
            self.client.tool.labels.delete(list(ids))
        except ToolkitAPIError as e:
            if missing := {ExternalId.model_validate(item) for item in e.missing or []}:
                if existing := (set(ids) - missing):
                    self.client.tool.labels.delete(list(existing))
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
    ) -> Iterable[LabelResponse]:
        filter: ClassicFilter | None = None
        if data_set_external_id is not None:
            filter = ClassicFilter(data_set_ids=[ExternalId(external_id=data_set_external_id)])
        for items in self.client.tool.labels.iterate(filter=filter):
            yield from items

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceCRUD], Hashable]]:
        """Returns all items that this item requires.

        For example, a TimeSeries requires a DataSet, so this method would return the
        DatasetLoader and identifier of that dataset.
        """
        if "dataSetExternalId" in item:
            yield DataSetsCRUD, ExternalId(external_id=item["dataSetExternalId"])

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> LabelRequest:
        if ds_external_id := resource.pop("dataSetExternalId", None):
            resource["dataSetId"] = self.client.lookup.data_sets.id(ds_external_id, is_dry_run)
        return LabelRequest._load(resource)

    def dump_resource(self, resource: LabelResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_write().dump()
        if data_set_id := dumped.pop("dataSetId", None):
            dumped["dataSetExternalId"] = self.client.lookup.data_sets.external_id(data_set_id)
        return dumped
