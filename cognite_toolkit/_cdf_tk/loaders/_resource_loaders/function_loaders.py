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

from collections import defaultdict
from collections.abc import Hashable, Iterable, Sequence, Sized
from functools import lru_cache
from numbers import Number
from pathlib import Path
from typing import Any, cast, final
from zipfile import ZipFile

from cognite.client.data_classes import (
    ClientCredentials,
    Function,
    FunctionList,
    FunctionSchedule,
    FunctionSchedulesList,
    FunctionScheduleWrite,
    FunctionScheduleWriteList,
    FunctionWrite,
    FunctionWriteList,
)
from cognite.client.data_classes.capabilities import (
    Capability,
    FilesAcl,
    FunctionsAcl,
    SessionsAcl,
)
from cognite.client.utils.useful_types import SequenceNotStr
from rich import print

from cognite_toolkit._cdf_tk._parameters import ParameterSpec, ParameterSpecSet
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitRequiredValueError,
)
from cognite_toolkit._cdf_tk.loaders._base_loaders import ResourceLoader
from cognite_toolkit._cdf_tk.utils import (
    CDFToolConfig,
    calculate_directory_hash,
    load_yaml_inject_variables,
)

from .auth_loaders import GroupAllScopedLoader
from .data_organization_loaders import DataSetsLoader


@final
class FunctionLoader(ResourceLoader[str, FunctionWrite, Function, FunctionWriteList, FunctionList]):
    support_drop = True
    folder_name = "functions"
    filename_pattern = (
        r"^(?:(?!schedule).)*$"  # Matches all yaml files except file names who's stem contain *.schedule.
    )
    resource_cls = Function
    resource_write_cls = FunctionWrite
    list_cls = FunctionList
    list_write_cls = FunctionWriteList
    kind = "Function"
    dependencies = frozenset({DataSetsLoader, GroupAllScopedLoader})
    _doc_url = "Functions/operation/postFunctions"

    @classmethod
    def get_required_capability(cls, items: FunctionWriteList) -> list[Capability] | list[Capability]:
        if not items:
            return []
        return [
            FunctionsAcl([FunctionsAcl.Action.Read, FunctionsAcl.Action.Write], FunctionsAcl.Scope.All()),
            FilesAcl(
                [FilesAcl.Action.Read, FilesAcl.Action.Write], FilesAcl.Scope.All()
            ),  # Needed for uploading function artifacts
        ]

    @classmethod
    def get_id(cls, item: Function | FunctionWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if item.external_id is None:
            raise ToolkitRequiredValueError("Function must have external_id set.")
        return item.external_id

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        if "dataSetExternalId" in item:
            yield DataSetsLoader, item["dataSetExternalId"]

    def load_resource(
        self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> FunctionWrite | FunctionWriteList | None:
        if filepath.parent.name != self.folder_name:
            # Functions configs needs to be in the root function folder.
            # Thi is to allow arbitrary YAML files inside the function code folder.
            return None

        functions = load_yaml_inject_variables(filepath, ToolGlobals.environment_variables())

        if isinstance(functions, dict):
            functions = [functions]

        for func in functions:
            if self.extra_configs.get(func["externalId"]) is None:
                self.extra_configs[func["externalId"]] = {}
            if func.get("dataSetExternalId") is not None:
                self.extra_configs[func["externalId"]]["dataSetId"] = ToolGlobals.verify_dataset(
                    func.get("dataSetExternalId", ""),
                    skip_validation=skip_validation,
                    action="replace datasetExternalId with dataSetId in function",
                )
            if "fileId" not in func:
                # The fileID is required for the function to be created, but in the `.create` method
                # we first create that file and then set the fileID.
                func["fileId"] = "<will_be_generated>"

        if len(functions) == 1:
            return FunctionWrite.load(functions[0])
        else:
            return FunctionWriteList.load(functions)

    def _are_equal(
        self, local: FunctionWrite, cdf_resource: Function, return_dumped: bool = False
    ) -> bool | tuple[bool, dict[str, Any], dict[str, Any]]:
        if self.resource_build_path is None:
            raise ValueError("build_path must be set to compare functions as function code must be compared.")
        # If the function failed, we want to always trigger a redeploy.
        if cdf_resource.status == "Failed":
            if return_dumped:
                return False, local.dump(), {}
            else:
                return False
        function_rootdir = Path(self.resource_build_path / f"{local.external_id}")
        if local.metadata is None:
            local.metadata = {}
        local.metadata["cdf-toolkit-function-hash"] = calculate_directory_hash(function_rootdir)

        # Is changed as part of deploy to the API
        local.file_id = cdf_resource.file_id
        cdf_resource.secrets = local.secrets
        # Set empty values for local
        attrs = [
            attr for attr in dir(cdf_resource) if not callable(getattr(cdf_resource, attr)) and not attr.startswith("_")
        ]
        # Remove server-side attributes
        attrs.remove("created_time")
        attrs.remove("error")
        attrs.remove("id")
        attrs.remove("runtime_version")
        attrs.remove("status")
        # Set empty values for local that have default values server-side
        for attribute in attrs:
            if getattr(local, attribute) is None:
                setattr(local, attribute, getattr(cdf_resource, attribute))
        local_dumped = local.dump()
        cdf_dumped = cdf_resource.as_write().dump()
        if local_dumped.get("dataSetId") == -1 and "dataSetId" in cdf_dumped:
            # Dry run
            local_dumped["dataSetId"] = cdf_dumped["dataSetId"]
        return self._return_are_equal(local_dumped, cdf_dumped, return_dumped)

    def retrieve(self, ids: SequenceNotStr[str]) -> FunctionList:
        status = self.client.functions.status()
        if status.status != "activated":
            if status.status == "requested":
                print(
                    "  [bold yellow]WARNING:[/] Function service activation is in progress, cannot retrieve functions."
                )
                return FunctionList([])
            else:
                print(
                    "  [bold yellow]WARNING:[/] Function service has not been activated, activating now, this may take up to 2 hours..."
                )
                self.client.functions.activate()
                return FunctionList([])
        ret = self.client.functions.retrieve_multiple(
            external_ids=cast(SequenceNotStr[str], ids), ignore_unknown_ids=True
        )
        if ret is None:
            return FunctionList([])
        if isinstance(ret, Function):
            return FunctionList([ret])
        else:
            return ret

    def update(self, items: FunctionWriteList) -> FunctionList:
        self.delete([item.external_id for item in items])
        return self.create(items)

    def _zip_and_upload_folder(
        self,
        root_dir: Path,
        external_id: str,
        data_set_id: int | None = None,
    ) -> int:
        zip_path = Path(root_dir.parent / f"{external_id}.zip")
        root_length = len(root_dir.parts)
        with ZipFile(zip_path, "w") as zipfile:
            for file in root_dir.rglob("*"):
                if file.is_file():
                    zipfile.write(file, "/".join(file.parts[root_length - 1 : -1]) + f"/{file.name}")
        file_info = self.client.files.upload_bytes(
            zip_path.read_bytes(),
            name=f"{external_id}.zip",
            external_id=external_id,
            overwrite=True,
            data_set_id=data_set_id,
        )
        zip_path.unlink()
        return cast(int, file_info.id)

    def create(self, items: Sequence[FunctionWrite]) -> FunctionList:
        items = list(items)
        created = FunctionList([], cognite_client=self.client)
        status = self.client.functions.status()
        if status.status != "activated":
            if status.status == "requested":
                print("  [bold yellow]WARNING:[/] Function service activation is in progress, skipping functions.")
                return FunctionList([])
            else:
                print(
                    "  [bold yellow]WARNING:[/] Function service is not activated, activating and skipping functions..."
                )
                self.client.functions.activate()
                return FunctionList([])
        if self.resource_build_path is None:
            raise ValueError("build_path must be set to compare functions as function code must be compared.")
        for item in items:
            function_rootdir = Path(self.resource_build_path / (item.external_id or ""))
            if item.metadata is None:
                item.metadata = {}
            item.metadata["cdf-toolkit-function-hash"] = calculate_directory_hash(function_rootdir)
            file_id = self._zip_and_upload_folder(
                root_dir=function_rootdir,
                external_id=item.external_id or item.name,
                data_set_id=self.extra_configs[item.external_id or item.name].get("dataSetId", None),
            )
            created.append(
                self.client.functions.create(
                    name=item.name,
                    external_id=item.external_id or item.name,
                    file_id=file_id,
                    function_path=item.function_path or "./handler.py",
                    description=item.description,
                    owner=item.owner,
                    secrets=item.secrets,
                    env_vars=item.env_vars,
                    cpu=cast(Number, item.cpu),
                    memory=cast(Number, item.memory),
                    runtime=item.runtime,
                    metadata=item.metadata,
                    index_url=item.index_url,
                    extra_index_urls=item.extra_index_urls,
                )
            )
        return created

    def delete(self, ids: SequenceNotStr[str]) -> int:
        self.client.functions.delete(external_id=cast(SequenceNotStr[str], ids))
        return len(ids)

    def iterate(self) -> Iterable[Function]:
        return iter(self.client.functions)

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # Added by toolkit
        spec.add(ParameterSpec(("dataSetExternalId",), frozenset({"str"}), is_required=False, _is_nullable=False))
        # Replaced by the toolkit
        spec.discard(ParameterSpec(("fileId",), frozenset({"int"}), is_required=True, _is_nullable=False))
        return spec


@final
class FunctionScheduleLoader(
    ResourceLoader[str, FunctionScheduleWrite, FunctionSchedule, FunctionScheduleWriteList, FunctionSchedulesList]
):
    folder_name = "functions"
    filename_pattern = r"^.*schedule.*$"  # Matches all yaml files who's stem contain *.schedule
    resource_cls = FunctionSchedule
    resource_write_cls = FunctionScheduleWrite
    list_cls = FunctionSchedulesList
    list_write_cls = FunctionScheduleWriteList
    kind = "Schedule"
    dependencies = frozenset({FunctionLoader})
    _doc_url = "Function-schedules/operation/postFunctionSchedules"
    _split_character = ":"

    @property
    def display_name(self) -> str:
        return "function.schedules"

    @classmethod
    def get_required_capability(cls, items: FunctionScheduleWriteList) -> list[Capability]:
        if not items:
            return []
        return [
            FunctionsAcl([FunctionsAcl.Action.Read, FunctionsAcl.Action.Write], FunctionsAcl.Scope.All()),
            SessionsAcl(
                [SessionsAcl.Action.List, SessionsAcl.Action.Create, SessionsAcl.Action.Delete], SessionsAcl.Scope.All()
            ),
        ]

    @classmethod
    def get_id(cls, item: FunctionScheduleWrite | FunctionSchedule | dict) -> str:
        if isinstance(item, dict):
            if missing := tuple(k for k in {"functionExternalId", "cronExpression"} if k not in item):
                # We need to raise a KeyError with all missing keys to get the correct error message.
                raise KeyError(*missing)
            return f"{item['functionExternalId']}{cls._split_character}{item['cronExpression']}"

        if item.function_external_id is None or item.cron_expression is None:
            raise ToolkitRequiredValueError("FunctionSchedule must have functionExternalId and CronExpression set.")
        return f"{item.function_external_id}{cls._split_character}{item.cron_expression}"

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        if "functionExternalId" in item:
            yield FunctionLoader, item["functionExternalId"]

    def load_resource(
        self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> FunctionScheduleWrite | FunctionScheduleWriteList | None:
        schedules = load_yaml_inject_variables(filepath, ToolGlobals.environment_variables())
        if isinstance(schedules, dict):
            schedules = [schedules]

        for sched in schedules:
            ext_id = f"{sched['functionExternalId']}{self._split_character}{sched['cronExpression']}"
            if self.extra_configs.get(ext_id) is None:
                self.extra_configs[ext_id] = {}
            self.extra_configs[ext_id]["authentication"] = sched.pop("authentication", {})
        return FunctionScheduleWriteList.load(schedules)

    def _are_equal(
        self, local: FunctionScheduleWrite, cdf_resource: FunctionSchedule, return_dumped: bool = False
    ) -> bool | tuple[bool, dict[str, Any], dict[str, Any]]:
        cdf_dumped = cdf_resource.as_write().dump()
        del cdf_dumped["functionId"]
        local_dumped = local.dump()
        return self._return_are_equal(local_dumped, cdf_dumped, return_dumped)

    def _resolve_functions_ext_id(self, items: FunctionScheduleWriteList) -> FunctionScheduleWriteList:
        functions = FunctionLoader(self.client, None).retrieve(list(set([item.function_external_id for item in items])))
        for item in items:
            for func in functions:
                if func.external_id == item.function_external_id:
                    item.function_id = func.id  # type: ignore[assignment]
        return items

    def retrieve(self, ids: SequenceNotStr[str]) -> FunctionSchedulesList:
        crons_by_function: dict[str, set[str]] = defaultdict(set)
        for id_ in ids:
            function_external_id, cron = id_.rsplit(self._split_character, 1)
            crons_by_function[function_external_id].add(cron)
        functions = FunctionLoader(self.client, None).retrieve(list(crons_by_function))
        schedules = FunctionSchedulesList([])
        for func in functions:
            ret = self.client.functions.schedules.list(function_id=func.id, limit=-1)
            for schedule in ret:
                schedule.function_external_id = func.external_id
            schedules.extend(
                [
                    schedule
                    for schedule in ret
                    if schedule.cron_expression in crons_by_function[cast(str, func.external_id)]
                ]
            )
        return schedules

    def create(self, items: FunctionScheduleWriteList) -> FunctionSchedulesList:
        items = self._resolve_functions_ext_id(items)
        created = []
        for item in items:
            key = f"{item.function_external_id}:{item.cron_expression}"
            auth_config = self.extra_configs.get(key, {}).get("authentication", {})
            if "clientId" in auth_config and "clientSecret" in auth_config:
                client_credentials = ClientCredentials(auth_config["clientId"], auth_config["clientSecret"])
            else:
                client_credentials = None

            created.append(
                self.client.functions.schedules.create(
                    name=item.name or "",
                    description=item.description or "",
                    cron_expression=cast(str, item.cron_expression),
                    function_id=cast(int, item.function_id),
                    data=item.data,
                    client_credentials=client_credentials,
                )
            )
        return FunctionSchedulesList(created)

    def update(self, items: FunctionScheduleWriteList) -> Sized:
        # Function schedule does not have an update, so we delete and recreate
        self.delete(self.get_ids(items))
        return self.create(items)

    def delete(self, ids: SequenceNotStr[str]) -> int:
        schedules = self.retrieve(ids)
        count = 0
        for schedule in schedules:
            if schedule.id:
                self.client.functions.schedules.delete(id=schedule.id)
            count += 1
        return count

    def iterate(self) -> Iterable[FunctionSchedule]:
        return iter(self.client.functions.schedules)

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # Added by toolkit
        spec.add(ParameterSpec(("authentication",), frozenset({"dict"}), is_required=False, _is_nullable=False))
        spec.add(
            ParameterSpec(("authentication", "clientId"), frozenset({"str"}), is_required=False, _is_nullable=False)
        )
        spec.add(
            ParameterSpec(("authentication", "clientSecret"), frozenset({"str"}), is_required=False, _is_nullable=False)
        )
        return spec
