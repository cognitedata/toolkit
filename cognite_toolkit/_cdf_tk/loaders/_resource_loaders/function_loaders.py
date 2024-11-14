import time
from collections import defaultdict
from collections.abc import Hashable, Iterable, Sized
from functools import lru_cache
from pathlib import Path
from typing import Any, cast, final

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
from cognite_toolkit._cdf_tk.client.data_classes.functions import FunctionScheduleID
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitRequiredValueError,
)
from cognite_toolkit._cdf_tk.loaders._base_loaders import ResourceLoader
from cognite_toolkit._cdf_tk.tk_warnings import HighSeverityWarning, LowSeverityWarning
from cognite_toolkit._cdf_tk.utils import (
    CDFToolConfig,
    calculate_directory_hash,
    calculate_secure_hash,
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
    do_environment_variable_injection = True

    class _MetadataKey:
        function_hash = "cdf-toolkit-function-hash"
        secret_hash = "cdf-toolkit-secret-hash"

    @classmethod
    def get_required_capability(
        cls, items: FunctionWriteList | None, read_only: bool
    ) -> list[Capability] | list[Capability]:
        if not items and items is not None:
            return []

        function_actions = (
            [FunctionsAcl.Action.Read] if read_only else [FunctionsAcl.Action.Read, FunctionsAcl.Action.Write]
        )
        file_actions = [FilesAcl.Action.Read] if read_only else [FilesAcl.Action.Read, FilesAcl.Action.Write]

        return [
            FunctionsAcl(function_actions, FunctionsAcl.Scope.All()),
            FilesAcl(file_actions, FilesAcl.Scope.All()),  # Needed for uploading function artifacts
        ]

    @classmethod
    def get_id(cls, item: Function | FunctionWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if item.external_id is None:
            raise ToolkitRequiredValueError("Function must have external_id set.")
        return item.external_id

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"externalId": id}

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        if "dataSetExternalId" in item:
            yield DataSetsLoader, item["dataSetExternalId"]

    def load_resource(
        self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> FunctionWrite | FunctionWriteList | None:
        if filepath.parent.name != self.folder_name:
            # Functions configs needs to be in the root function folder.
            # This is to allow arbitrary YAML files inside the function code folder.
            return None

        use_environment_variables = (
            ToolGlobals.environment_variables() if self.do_environment_variable_injection else {}
        )
        functions = load_yaml_inject_variables(filepath, use_environment_variables)

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
        local.metadata[self._MetadataKey.function_hash] = calculate_directory_hash(
            function_rootdir, ignore_files={".pyc"}
        )

        # Is changed as part of deployment to the API
        local.file_id = cdf_resource.file_id
        if cdf_resource.cpu and local.cpu is None:
            local.cpu = cdf_resource.cpu
        if cdf_resource.memory and local.memory is None:
            local.memory = cdf_resource.memory
        if cdf_resource.runtime and local.runtime is None:
            local.runtime = cdf_resource.runtime

        local_dumped = local.dump()
        cdf_dumped = cdf_resource.as_write().dump()
        if local_dumped.get("dataSetId") == -1 and "dataSetId" in cdf_dumped:
            # Dry run
            local_dumped["dataSetId"] = cdf_dumped["dataSetId"]

        if "secrets" in local_dumped:
            local_dumped["metadata"][self._MetadataKey.secret_hash] = calculate_secure_hash(local_dumped["secrets"])
            local_dumped["secrets"] = {k: "***" for k in local_dumped["secrets"]}

        return self._return_are_equal(local_dumped, cdf_dumped, return_dumped)

    def _is_activated(self, action: str) -> bool:
        status = self.client.functions.status()
        if status.status == "activated":
            return True
        if status.status == "requested":
            print(
                HighSeverityWarning(
                    f"Function service activation is in progress, cannot {action} functions."
                ).get_message()
            )
            return False
        else:
            print(
                HighSeverityWarning(
                    "Function service has not been activated, activating now, this may take up to 2 hours..."
                ).get_message()
            )
            self.client.functions.activate()
        return False

    def create(self, items: FunctionWriteList) -> FunctionList:
        created = FunctionList([], cognite_client=self.client)
        if not self._is_activated("create"):
            return created
        if self.resource_build_path is None:
            raise ValueError("build_path must be set to compare functions as function code must be compared.")
        for item in items:
            function_rootdir = Path(self.resource_build_path / (item.external_id or ""))
            item.metadata = item.metadata or {}
            item.metadata[self._MetadataKey.function_hash] = calculate_directory_hash(
                function_rootdir, ignore_files={".pyc"}
            )
            if item.secrets:
                item.metadata[self._MetadataKey.secret_hash] = calculate_secure_hash(item.secrets)

            file_id = self.client.functions._zip_and_upload_folder(
                name=item.name,
                folder=str(function_rootdir),
                external_id=item.external_id or item.name,
                data_set_id=self.extra_configs[item.external_id or item.name].get("dataSetId", None),
            )
            # Wait until the files is available
            sleep_time = 1.0  # seconds
            for i in range(5):
                file = self.client.files.retrieve(id=file_id)
                if file and file.uploaded:
                    break
                time.sleep(sleep_time)
                sleep_time *= 2
            else:
                raise RuntimeError("Could not retrieve file from files API")
            item.file_id = file_id
            created.append(self.client.functions.create(item))
        return created

    def retrieve(self, ids: SequenceNotStr[str]) -> FunctionList:
        if not self._is_activated("retrieve"):
            return FunctionList([])
        return self.client.functions.retrieve_multiple(external_ids=ids, ignore_unknown_ids=True)

    def update(self, items: FunctionWriteList) -> FunctionList:
        self.delete(items.as_external_ids())
        return self.create(items)

    def delete(self, ids: SequenceNotStr[str]) -> int:
        functions = self.retrieve(ids)

        self.client.functions.delete(external_id=ids)
        file_ids = {func.file_id for func in functions if func.file_id}
        self.client.files.delete(id=list(file_ids), ignore_unknown_ids=True)
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
    ResourceLoader[
        FunctionScheduleID, FunctionScheduleWrite, FunctionSchedule, FunctionScheduleWriteList, FunctionSchedulesList
    ]
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
    do_environment_variable_injection = True

    @property
    def display_name(self) -> str:
        return "function.schedules"

    @classmethod
    def get_required_capability(cls, items: FunctionScheduleWriteList | None, read_only: bool) -> list[Capability]:
        if not items and items is not None:
            return []

        function_actions = (
            [FunctionsAcl.Action.Read] if read_only else [FunctionsAcl.Action.Read, FunctionsAcl.Action.Write]
        )
        session_actions = (
            [SessionsAcl.Action.List]
            if read_only
            else [SessionsAcl.Action.List, SessionsAcl.Action.Create, SessionsAcl.Action.Delete]
        )

        return [
            FunctionsAcl(function_actions, FunctionsAcl.Scope.All()),
            SessionsAcl(session_actions, SessionsAcl.Scope.All()),
        ]

    @classmethod
    def dump_id(cls, id: FunctionScheduleID) -> dict[str, Any]:
        return id.dump(camel_case=True)

    @classmethod
    def get_id(cls, item: FunctionScheduleWrite | FunctionSchedule | dict) -> FunctionScheduleID:
        if isinstance(item, dict):
            if missing := tuple(k for k in {"functionExternalId", "name"} if k not in item):
                # We need to raise a KeyError with all missing keys to get the correct error message.
                raise KeyError(*missing)
            return FunctionScheduleID(item["functionExternalId"], item["name"])

        if item.function_external_id is None or item.name is None:
            raise ToolkitRequiredValueError("FunctionSchedule must have functionExternalId and Name set.")
        return FunctionScheduleID(item.function_external_id, item.name)

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        if "functionExternalId" in item:
            yield FunctionLoader, item["functionExternalId"]

    def load_resource(
        self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> FunctionScheduleWriteList:
        use_environment_variables = (
            ToolGlobals.environment_variables() if self.do_environment_variable_injection else {}
        )
        schedules = load_yaml_inject_variables(filepath, use_environment_variables)

        if isinstance(schedules, dict):
            schedules = [schedules]

        for schedule in schedules:
            identifier = self.get_id(schedule)
            if self.extra_configs.get(identifier) is None:
                self.extra_configs[identifier] = {}
            self.extra_configs[identifier]["authentication"] = schedule.pop("authentication", {})
            if "functionId" in schedule:
                LowSeverityWarning(
                    f"FunctionId will be ignored in the schedule {schedule.get('functionExternalId', 'Misssing')!r}"
                ).print_warning()
                schedule.pop("functionId", None)

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

    def retrieve(self, ids: SequenceNotStr[FunctionScheduleID]) -> FunctionSchedulesList:
        names_by_function: dict[str, set[str]] = defaultdict(set)
        for id_ in ids:
            names_by_function[id_.function_external_id].add(id_.name)
        functions = FunctionLoader(self.client, None).retrieve(list(names_by_function))
        schedules = FunctionSchedulesList([])
        for func in functions:
            func_external_id = cast(str, func.external_id)
            function_schedules = self.client.functions.schedules.list(function_id=func.id, limit=-1)
            for schedule in function_schedules:
                schedule.function_external_id = func_external_id
            schedules.extend(
                [schedule for schedule in function_schedules if schedule.name in names_by_function[func_external_id]]
            )
        return schedules

    def create(self, items: FunctionScheduleWriteList) -> FunctionSchedulesList:
        created_list = FunctionSchedulesList([], cognite_client=self.client)
        for item in items:
            id_ = self.get_id(item)
            auth_config = self.extra_configs.get(id_, {}).get("authentication", {})
            if "clientId" in auth_config and "clientSecret" in auth_config:
                client_credentials = ClientCredentials(auth_config["clientId"], auth_config["clientSecret"])
            else:
                client_credentials = None

            created = self.client.functions.schedules.create(item, client_credentials=client_credentials)
            # The PySDK mutates the input object, such that function_id is set and function_external_id is None.
            # If we call .get_id on the returned object, it will raise an error we require the function_external_id
            # to be set.
            created.function_external_id = id_.function_external_id
            created_list.append(created)
        return created_list

    def update(self, items: FunctionScheduleWriteList) -> Sized:
        # Function schedule does not have an update, so we delete and recreate
        self.delete(self.get_ids(items))
        return self.create(items)

    def delete(self, ids: SequenceNotStr[FunctionScheduleID]) -> int:
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
