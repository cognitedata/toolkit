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

import re
from collections.abc import Hashable, Iterable, Sequence
from functools import lru_cache
from pathlib import Path
from typing import Any, final

import yaml
from cognite.client.data_classes import (
    ExtractionPipeline,
    ExtractionPipelineConfig,
    ExtractionPipelineList,
)
from cognite.client.data_classes.capabilities import (
    Capability,
    ExtractionConfigsAcl,
    ExtractionPipelinesAcl,
)
from cognite.client.data_classes.extractionpipelines import (
    ExtractionPipelineConfigList,
    ExtractionPipelineConfigWrite,
    ExtractionPipelineConfigWriteList,
    ExtractionPipelineWrite,
    ExtractionPipelineWriteList,
)
from cognite.client.exceptions import CogniteAPIError, CogniteDuplicatedError, CogniteNotFoundError
from cognite.client.utils.useful_types import SequenceNotStr
from rich import print

from cognite_toolkit._cdf_tk._parameters import ANYTHING, ParameterSpec, ParameterSpecSet
from cognite_toolkit._cdf_tk.client.data_classes.raw import RawDatabase, RawTable
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitRequiredValueError,
)
from cognite_toolkit._cdf_tk.loaders._base_loaders import ResourceLoader
from cognite_toolkit._cdf_tk.tk_warnings import (
    HighSeverityWarning,
)
from cognite_toolkit._cdf_tk.utils import (
    load_yaml_inject_variables,
    read_yaml_content,
    safe_read,
    stringify_value_by_key_in_yaml,
)
from cognite_toolkit._cdf_tk.utils.diff_list import diff_list_force_hashable, diff_list_identifiable

from .auth_loaders import GroupAllScopedLoader
from .data_organization_loaders import DataSetsLoader
from .raw_loaders import RawDatabaseLoader, RawTableLoader


@final
class ExtractionPipelineLoader(
    ResourceLoader[
        str, ExtractionPipelineWrite, ExtractionPipeline, ExtractionPipelineWriteList, ExtractionPipelineList
    ]
):
    folder_name = "extraction_pipelines"
    filename_pattern = r"^(?:(?!\.config).)*$"  # Matches all yaml files except file names who's stem contain *.config.
    resource_cls = ExtractionPipeline
    resource_write_cls = ExtractionPipelineWrite
    list_cls = ExtractionPipelineList
    list_write_cls = ExtractionPipelineWriteList
    kind = "ExtractionPipeline"
    dependencies = frozenset({DataSetsLoader, RawDatabaseLoader, RawTableLoader, GroupAllScopedLoader})
    _doc_url = "Extraction-Pipelines/operation/createExtPipes"

    @property
    def display_name(self) -> str:
        return "extraction pipelines"

    @classmethod
    def get_required_capability(
        cls, items: Sequence[ExtractionPipelineWrite] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []

        actions = (
            [ExtractionPipelinesAcl.Action.Read]
            if read_only
            else [ExtractionPipelinesAcl.Action.Read, ExtractionPipelinesAcl.Action.Write]
        )

        scope: ExtractionPipelinesAcl.Scope.All | ExtractionPipelinesAcl.Scope.DataSet = (  # type: ignore[valid-type]
            ExtractionPipelinesAcl.Scope.All()
        )
        if items is not None:
            if data_set_id := {item.data_set_id for item in items if item.data_set_id}:
                scope = ExtractionPipelinesAcl.Scope.DataSet(list(data_set_id))

        return ExtractionPipelinesAcl(
            actions,
            scope,  # type: ignore[arg-type]
        )

    @classmethod
    def get_id(cls, item: ExtractionPipeline | ExtractionPipelineWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if item.external_id is None:
            raise ToolkitRequiredValueError("ExtractionPipeline must have external_id set.")
        return item.external_id

    @classmethod
    def get_internal_id(cls, item: ExtractionPipeline | dict) -> int:
        if isinstance(item, dict):
            return item["id"]
        if item.id is None:
            raise ToolkitRequiredValueError("ExtractionPipeline must have id set.")
        return item.id

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"externalId": id}

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        seen_databases: set[str] = set()
        if "dataSetExternalId" in item:
            yield DataSetsLoader, item["dataSetExternalId"]
        if "rawTables" in item:
            for entry in item["rawTables"]:
                if db := entry.get("dbName"):
                    if db not in seen_databases:
                        seen_databases.add(db)
                        yield RawDatabaseLoader, RawDatabase(db_name=db)
                    if "tableName" in entry:
                        yield RawTableLoader, RawTable._load(entry)

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> ExtractionPipelineWrite:
        if ds_external_id := resource.pop("dataSetExternalId", None):
            resource["dataSetId"] = self.client.lookup.data_sets.id(ds_external_id, is_dry_run)
        if "createdBy" not in resource:
            # Todo; Bug SDK missing default value (this will be set on the server-side if missing)
            resource["createdBy"] = "unknown"
        return ExtractionPipelineWrite._load(resource)

    def dump_resource(self, resource: ExtractionPipeline, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_write().dump()
        local = local or {}
        if data_set_id := dumped.pop("dataSetId", None):
            dumped["dataSetExternalId"] = self.client.lookup.data_sets.external_id(data_set_id)
        if dumped.get("createdBy") == "unknown" and "createdBy" not in local:
            dumped.pop("createdBy", None)
        return dumped

    def diff_list(
        self, local: list[Any], cdf: list[Any], json_path: tuple[str | int, ...]
    ) -> tuple[dict[int, int], list[int]]:
        if json_path == ("rawTables",):
            return diff_list_identifiable(local, cdf, get_identifier=lambda x: (x["dbName"], x["tableName"]))
        return super().diff_list(local, cdf, json_path)

    def create(self, items: Sequence[ExtractionPipelineWrite]) -> ExtractionPipelineList:
        items = list(items)
        try:
            return self.client.extraction_pipelines.create(items)
        except CogniteDuplicatedError as e:
            if len(e.duplicated) < len(items):
                for dup in e.duplicated:
                    ext_id = dup.get("externalId", None)
                    for item in items:
                        if item.external_id == ext_id:
                            items.remove(item)

                return self.client.extraction_pipelines.create(items)
        return ExtractionPipelineList([])

    def retrieve(self, ids: SequenceNotStr[str]) -> ExtractionPipelineList:
        return self.client.extraction_pipelines.retrieve_multiple(external_ids=ids, ignore_unknown_ids=True)

    def update(self, items: ExtractionPipelineWriteList) -> ExtractionPipelineList:
        # Bug in SDK overload so need the ignore.
        return self.client.extraction_pipelines.update(items, mode="replace")  # type: ignore[call-overload]

    def delete(self, ids: SequenceNotStr[str | int]) -> int:
        internal_ids, external_ids = self._split_ids(ids)
        try:
            self.client.extraction_pipelines.delete(id=internal_ids, external_id=external_ids)
        except CogniteNotFoundError as e:
            not_existing = {external_id for dup in e.not_found if (external_id := dup.get("externalId", None))}
            if id_list := [id_ for id_ in ids if id_ not in not_existing]:
                internal_ids, external_ids = self._split_ids(id_list)
                self.client.extraction_pipelines.delete(id=internal_ids, external_id=external_ids)
        except CogniteAPIError as e:
            if e.code == 403 and "not found" in e.message and "extraction pipeline" in e.message.lower():
                return 0
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[ExtractionPipeline]:
        if data_set_external_id is None:
            yield from iter(self.client.extraction_pipelines)
            return
        data_set = self.client.data_sets.retrieve(external_id=data_set_external_id)
        if data_set is None:
            raise ToolkitRequiredValueError(f"DataSet {data_set_external_id!r} does not exist")
        for pipeline in self.client.extraction_pipelines:
            if pipeline.data_set_id == data_set.id:
                yield pipeline

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # Added by toolkit
        spec.add(ParameterSpec(("dataSetExternalId",), frozenset({"str"}), is_required=False, _is_nullable=False))
        # Set on deploy time by toolkit
        spec.discard(ParameterSpec(("dataSetId",), frozenset({"int"}), is_required=True, _is_nullable=False))
        return spec


@final
class ExtractionPipelineConfigLoader(
    ResourceLoader[
        str,
        ExtractionPipelineConfigWrite,
        ExtractionPipelineConfig,
        ExtractionPipelineConfigWriteList,
        ExtractionPipelineConfigList,
    ]
):
    folder_name = "extraction_pipelines"
    filename_pattern = r"^.*config$"
    resource_cls = ExtractionPipelineConfig
    resource_write_cls = ExtractionPipelineConfigWrite
    list_cls = ExtractionPipelineConfigList
    list_write_cls = ExtractionPipelineConfigWriteList
    kind = "Config"
    dependencies = frozenset({ExtractionPipelineLoader})
    _doc_url = "Extraction-Pipelines-Config/operation/createExtPipeConfig"

    @property
    def display_name(self) -> str:
        return "extraction pipeline configs"

    @classmethod
    def get_required_capability(
        cls, items: Sequence[ExtractionPipelineConfigWrite] | None, read_only: bool
    ) -> list[Capability]:
        if not items and items is not None:
            return []

        actions = (
            [ExtractionConfigsAcl.Action.Read]
            if read_only
            else [ExtractionConfigsAcl.Action.Read, ExtractionConfigsAcl.Action.Write]
        )

        return [
            ExtractionConfigsAcl(
                actions,
                ExtractionConfigsAcl.Scope.All(),
            )
        ]

    @classmethod
    def get_id(cls, item: ExtractionPipelineConfig | ExtractionPipelineConfigWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if item.external_id is None:
            raise ToolkitRequiredValueError("ExtractionPipelineConfig must have external_id set.")
        return item.external_id

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"externalId": id}

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        if "externalId" in item:
            yield ExtractionPipelineLoader, item["externalId"]

    def safe_read(self, filepath: Path | str) -> str:
        # The config is expected to be a string that is parsed as a YAML on the server side.
        # The user typically writes the config as an object, so add a | to ensure it is parsed as a string.
        return stringify_value_by_key_in_yaml(safe_read(filepath), key="config")

    def load_resource_file(
        # special case where the environment variable keys in the 'config' value
        # should not be replaced but preserved as is
        self,
        filepath: Path,
        environment_variables: dict[str, str | None] | None = None,
    ) -> list[dict[str, Any]]:
        raw_str = self.safe_read(filepath)

        original = load_yaml_inject_variables(raw_str, {}, validate=False, original_filepath=filepath)
        replaced = load_yaml_inject_variables(
            raw_str, environment_variables or {}, validate=False, original_filepath=filepath
        )

        if isinstance(original, dict) and isinstance(replaced, dict):
            if "config" in original:
                replaced["config"] = original.get("config")
                return [replaced]

        for orig_item, repl_item in zip(original, replaced):
            if "config" in orig_item:
                repl_item["config"] = orig_item.get("config")  # type: ignore
        return replaced if isinstance(replaced, list) else [replaced]

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> ExtractionPipelineConfigWrite:
        config_raw = resource.get("config")
        if isinstance(config_raw, str):
            # There might be keyvauls secrets in the config that would lead to parsing errors. The syntax
            # for this is `connection-string: !keyvault secret`. This is not valid YAML, so we need to
            # replace it with `connection-string: keyvault secret` to make it valid.
            config_raw = re.sub(r": !(\w+)", r": \1", config_raw)
            try:
                read_yaml_content(config_raw)
            except yaml.YAMLError as e:
                print(
                    HighSeverityWarning(
                        f"Configuration for {resource.get('external_id', 'missing')} could not be parsed "
                        f"as valid YAML, which is the recommended format. Error: {e}"
                    ).get_message()
                )
        return ExtractionPipelineConfigWrite._load(resource)

    def diff_list(
        self, local: list[Any], cdf: list[Any], json_path: tuple[str | int, ...]
    ) -> tuple[dict[int, int], list[int]]:
        if json_path[0] == "config":
            # Assume all arrays in the config are hashable
            return diff_list_force_hashable(local, cdf)
        return super().diff_list(local, cdf, json_path)

    def _upsert(self, items: ExtractionPipelineConfigWriteList) -> ExtractionPipelineConfigList:
        upserted = ExtractionPipelineConfigList([])
        for item in items:
            created = self.client.extraction_pipelines.config.create(item)
            upserted.append(created)
        return upserted

    def create(self, items: ExtractionPipelineConfigWriteList) -> ExtractionPipelineConfigList:
        return self._upsert(items)

    # configs cannot be updated, instead new revision is created
    def update(self, items: ExtractionPipelineConfigWriteList) -> ExtractionPipelineConfigList:
        return self._upsert(items)

    def retrieve(self, ids: SequenceNotStr[str]) -> ExtractionPipelineConfigList:
        retrieved = ExtractionPipelineConfigList([])
        for external_id in ids:
            try:
                config_retrieved = self.client.extraction_pipelines.config.retrieve(external_id=external_id)
            except CogniteAPIError as e:
                if (
                    e.code == 404
                    and e.message.startswith("There is no config stored for the extraction pipeline with external id")
                ) or e.message.startswith("Extraction pipeline not found"):
                    continue
                raise e
            if config_retrieved:
                retrieved.append(config_retrieved)
        return retrieved

    def delete(self, ids: SequenceNotStr[str]) -> int:
        """Delete is not supported for extraction pipeline configs.

        Instead, we assume that when the user deletes the extraction pipeline configs, they are also deleting the
        extraction pipelines which will automatically delete the configs. In this method, we simply count the number
        of configs that exist for the given ids and return that number as these will be deleted.
        """
        count = 0
        for id_ in ids:
            try:
                result = self.client.extraction_pipelines.config.list(external_id=id_)
            except CogniteAPIError as e:
                if e.code == 403 and "not found" in e.message and "extraction pipeline" in e.message.lower():
                    continue
            else:
                if result:
                    count += 1
        return count

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[ExtractionPipelineConfig]:
        parent_iterable = parent_ids or iter(self.client.extraction_pipelines)
        for parent_id in parent_iterable or []:
            pipeline_id: str | None = None
            if isinstance(parent_id, ExtractionPipeline):
                if parent_id.external_id:
                    pipeline_id = parent_id.external_id
            elif isinstance(parent_id, str):
                pipeline_id = parent_id

            if pipeline_id is None:
                continue

            try:
                yield self.client.extraction_pipelines.config.retrieve(external_id=pipeline_id)
            except CogniteAPIError as e:
                if e.code == 404 and "There is no config stored" in e.message:
                    continue

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # Added by toolkit
        spec.add(ParameterSpec(("config", ANYTHING), frozenset({"dict"}), is_required=True, _is_nullable=False))
        return spec
