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

from collections.abc import Hashable, Iterable, Sequence
from functools import lru_cache
from pathlib import Path
from typing import Any, cast, final

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
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitRequiredValueError,
)
from cognite_toolkit._cdf_tk.loaders._base_loaders import ResourceLoader
from cognite_toolkit._cdf_tk.loaders.data_classes import RawDatabaseTable
from cognite_toolkit._cdf_tk.tk_warnings import (
    HighSeverityWarning,
)
from cognite_toolkit._cdf_tk.utils import (
    CDFToolConfig,
    load_yaml_inject_variables,
    safe_read,
    stringify_value_by_key_in_yaml,
)

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

    @classmethod
    def get_required_capability(cls, items: ExtractionPipelineWriteList | None) -> Capability | list[Capability]:
        if not items and items is not None:
            return []
        scope: ExtractionPipelinesAcl.Scope.All | ExtractionPipelinesAcl.Scope.DataSet = (  # type: ignore[valid-type]
            ExtractionPipelinesAcl.Scope.All()
        )
        if items is not None:
            if data_set_id := {item.data_set_id for item in items if item.data_set_id}:
                scope = ExtractionPipelinesAcl.Scope.DataSet(list(data_set_id))

        return ExtractionPipelinesAcl(
            [ExtractionPipelinesAcl.Action.Read, ExtractionPipelinesAcl.Action.Write],
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
                        yield RawDatabaseLoader, RawDatabaseTable(db_name=db)
                    if "tableName" in entry:
                        yield RawTableLoader, RawDatabaseTable._load(entry)

    def load_resource(
        self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> ExtractionPipelineWrite | ExtractionPipelineWriteList:
        resources = load_yaml_inject_variables(filepath, {})
        if isinstance(resources, dict):
            resources = [resources]

        for resource in resources:
            if "dataSetExternalId" in resource:
                ds_external_id = resource.pop("dataSetExternalId")
                resource["dataSetId"] = ToolGlobals.verify_dataset(
                    ds_external_id,
                    skip_validation,
                    action="replace datasetExternalId with dataSetId in extraction pipeline",
                )
            if "createdBy" not in resource:
                # Todo; Bug SDK missing default value (this will be set on the server-side if missing)
                resource["createdBy"] = "unknown"

        if len(resources) == 1:
            return ExtractionPipelineWrite.load(resources[0])
        else:
            return ExtractionPipelineWriteList.load(resources)

    def _are_equal(
        self, local: ExtractionPipelineWrite, cdf_resource: ExtractionPipeline, return_dumped: bool = False
    ) -> bool | tuple[bool, dict[str, Any], dict[str, Any]]:
        local_dumped = local.dump()
        cdf_dumped = cdf_resource.as_write().dump()
        if local_dumped.get("dataSetId") == -1 and "dataSetId" in cdf_dumped:
            # Dry run
            local_dumped["dataSetId"] = cdf_dumped["dataSetId"]
        return self._return_are_equal(local_dumped, cdf_dumped, return_dumped)

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

    def delete(self, ids: SequenceNotStr[str]) -> int:
        id_list = list(ids)
        try:
            self.client.extraction_pipelines.delete(external_id=id_list)
        except CogniteNotFoundError as e:
            not_existing = {external_id for dup in e.not_found if (external_id := dup.get("externalId", None))}
            if id_list := [id_ for id_ in id_list if id_ not in not_existing]:
                self.client.extraction_pipelines.delete(external_id=id_list)
        except CogniteAPIError as e:
            if e.code == 403 and "not found" in e.message and "extraction pipeline" in e.message.lower():
                return 0
        return len(id_list)

    def iterate(self) -> Iterable[ExtractionPipeline]:
        return iter(self.client.extraction_pipelines)

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
        return "extraction_pipeline.config"

    @classmethod
    def get_required_capability(cls, items: ExtractionPipelineConfigWriteList | None) -> list[Capability]:
        if not items and items is not None:
            return []

        return [
            ExtractionConfigsAcl(
                [ExtractionConfigsAcl.Action.Read, ExtractionConfigsAcl.Action.Write],
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

    def load_resource(
        self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> ExtractionPipelineConfigWrite | ExtractionPipelineConfigWriteList:
        # The config is expected to be a string that is parsed as a YAML on the server side.
        # The user typically writes the config as an object, so add a | to ensure it is parsed as a string.
        raw_str = stringify_value_by_key_in_yaml(safe_read(filepath), key="config")
        resources = load_yaml_inject_variables(raw_str, {})
        if isinstance(resources, dict):
            resources = [resources]

        for resource in resources:
            config_raw = resource.get("config")
            if isinstance(config_raw, str):
                try:
                    yaml.safe_load(config_raw)
                except yaml.YAMLError as e:
                    print(
                        HighSeverityWarning(
                            f"Configuration for {resource.get('external_id', 'missing')} could not be parsed "
                            f"as valid YAML, which is the recommended format. Error: {e}"
                        ).get_message()
                    )
        if len(resources) == 1:
            return ExtractionPipelineConfigWrite.load(resources[0])
        else:
            return ExtractionPipelineConfigWriteList.load(resources)

    def _upsert(self, items: ExtractionPipelineConfigWriteList) -> ExtractionPipelineConfigList:
        updated = ExtractionPipelineConfigList([])
        for item in items:
            if not item.external_id:
                raise ToolkitRequiredValueError("ExtractionPipelineConfig must have external_id set.")
            try:
                latest = self.client.extraction_pipelines.config.retrieve(item.external_id)
            except CogniteAPIError:
                latest = None
            if latest and self._are_equal(item, latest):
                updated.append(latest)
                continue
            else:
                created = self.client.extraction_pipelines.config.create(item)
                updated.append(created)
        return updated

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
                    or e.message.startswith("Extraction pipeline not found")
                ):
                    continue
                raise e
            if config_retrieved:
                retrieved.append(config_retrieved)
        return retrieved

    def delete(self, ids: SequenceNotStr[str]) -> int:
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

    def iterate(self) -> Iterable[ExtractionPipelineConfig]:
        return (
            self.client.extraction_pipelines.config.retrieve(external_id=cast(str, pipeline.external_id))
            for pipeline in self.client.extraction_pipelines
        )

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # Added by toolkit
        spec.add(ParameterSpec(("config", ANYTHING), frozenset({"dict"}), is_required=True, _is_nullable=False))
        return spec
