import json
from collections.abc import Hashable, Iterable
from functools import lru_cache
from pathlib import Path
from typing import Any, cast, final

from cognite.client import _version as CogniteSDKVersion
from cognite.client.data_classes.capabilities import (
    Capability,
    FilesAcl,
)
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk._parameters import ParameterSpec, ParameterSpecSet
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.streamlit_ import (
    Streamlit,
    StreamlitList,
    StreamlitWrite,
    StreamlitWriteList,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotADirectoryError, ToolkitRequiredValueError
from cognite_toolkit._cdf_tk.loaders._base_loaders import ResourceLoader
from cognite_toolkit._cdf_tk.utils import (
    CDFToolConfig,
    load_yaml_inject_variables,
)
from cognite_toolkit._cdf_tk.utils.hashing import calculate_str_or_file_hash

from .auth_loaders import GroupAllScopedLoader
from .data_organization_loaders import DataSetsLoader


@final
class StreamlitLoader(ResourceLoader[str, StreamlitWrite, Streamlit, StreamlitWriteList, StreamlitList]):
    folder_name = "streamlit"
    filename_pattern = r".*streamlit$"
    resource_cls = Streamlit
    resource_write_cls = StreamlitWrite
    list_cls = StreamlitList
    list_write_cls = StreamlitWriteList
    kind = "Streamlit"
    dependencies = frozenset({DataSetsLoader, GroupAllScopedLoader})
    _doc_url = "Files/operation/initFileUpload"
    _metadata_hash_key = "cdf-toolkit-app-hash"

    def __init__(self, client: ToolkitClient, build_dir: Path | None):
        super().__init__(client, build_dir)
        self._source_file_by_external_id: dict[str, Path] = {}

    @classmethod
    def get_required_capability(
        cls, items: StreamlitWriteList | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []

        actions = [FilesAcl.Action.Read] if read_only else [FilesAcl.Action.Read, FilesAcl.Action.Write]

        scope: FilesAcl.Scope.All | FilesAcl.Scope.DataSet = FilesAcl.Scope.All()  # type: ignore[valid-type]
        if items:
            if data_set_ids := {item.data_set_id for item in items if item.data_set_id}:
                scope = FilesAcl.Scope.DataSet(list(data_set_ids))

        return FilesAcl(actions, scope)  # type: ignore[arg-type]

    @classmethod
    def get_id(cls, item: Streamlit | StreamlitWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if item.external_id is None:
            raise ToolkitRequiredValueError("TimeSeries must have external_id set.")
        return item.external_id

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"externalId": id}

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        if "dataSetExternalId" in item:
            yield DataSetsLoader, item["dataSetExternalId"]

    def load_resource(self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool) -> StreamlitWriteList:
        use_environment_variables = (
            ToolGlobals.environment_variables() if self.do_environment_variable_injection else {}
        )
        resources = load_yaml_inject_variables(filepath, use_environment_variables)

        if not isinstance(resources, list):
            resources = [resources]
        for resource in resources:
            if resource.get("dataSetExternalId") is not None:
                ds_external_id = resource.pop("dataSetExternalId")
                resource["dataSetId"] = ToolGlobals.verify_dataset(
                    ds_external_id, skip_validation, action="replace dataSetExternalId with dataSetId in streamlit"
                )
        loaded = cast(StreamlitWriteList, StreamlitWriteList._load(resources))
        for item in loaded:
            self._source_file_by_external_id[item.external_id] = filepath
        return loaded

    def _are_equal(
        self, local: StreamlitWrite, cdf_resource: Streamlit, return_dumped: bool = False
    ) -> bool | tuple[bool, dict[str, Any], dict[str, Any]]:
        local_hash = calculate_str_or_file_hash(self._as_json_string(local.external_id, local.entrypoint), shorten=True)
        local_dumped = local.dump()
        local_dumped[self._metadata_hash_key] = local_hash
        cdf_dumped = cdf_resource.as_write().dump()
        cdf_dumped[self._metadata_hash_key] = cdf_resource.app_hash

        # If dataSetId is not set in the local, but are set in the CDF, it is a dry run
        # and we assume they are the same.
        if local_dumped.get("dataSetId") == -1 and "dataSetId" in cdf_dumped:
            local_dumped["dataSetId"] = cdf_dumped["dataSetId"]
        return self._return_are_equal(local_dumped, cdf_dumped, return_dumped)

    @lru_cache
    def _as_json_string(self, external_id: str, entrypoint: str) -> str:
        source_file = self._source_file_by_external_id[external_id]
        app_path = source_file.with_name(external_id)
        if not app_path.exists():
            raise ToolkitNotADirectoryError(f"Streamlit app folder does not exists. Expected: {app_path}")
        requirements_txt = app_path / "requirements.txt"
        if requirements_txt.exists():
            requirements_txt_lines = requirements_txt.read_text().splitlines()
        else:
            requirements_txt_lines = ["pyodide-http==0.2.1", f"cognite-sdk=={CogniteSDKVersion.__version__}"]
        files = {
            py_file.relative_to(app_path).as_posix(): {"content": {"text": py_file.read_text(), "$case": "text"}}
            for py_file in app_path.rglob("*.py")
            if py_file.is_file()
        }

        return json.dumps(
            {
                "entrypoint": entrypoint,
                "files": files,
                "requirements": requirements_txt_lines,
            }
        )

    def create(self, items: StreamlitWriteList) -> StreamlitList:
        created = StreamlitList([])
        for item in items:
            content = self._as_json_string(item.external_id, item.entrypoint)
            to_create = item.as_file()
            to_create.metadata[self._metadata_hash_key] = calculate_str_or_file_hash(content, shorten=True)  # type: ignore[index]
            created_file, _ = self.client.files.create(to_create)

            self.client.files.upload_content_bytes(content, item.external_id)
            created.append(Streamlit.from_file(created_file))
        return created

    def retrieve(self, ids: SequenceNotStr[str]) -> StreamlitList:
        files = self.client.files.retrieve_multiple(external_ids=ids, ignore_unknown_ids=True)
        return StreamlitList([Streamlit.from_file(file) for file in files])

    def update(self, items: StreamlitWriteList) -> StreamlitList:
        files = items.as_file_list()
        updated = self.client.files.update(files, mode="replace")
        return StreamlitList([Streamlit.from_file(file) for file in updated])

    def delete(self, ids: SequenceNotStr[str]) -> int:
        self.client.files.delete(external_id=ids)
        return len(ids)

    def iterate(self) -> Iterable[Streamlit]:
        for file in self.client.files:
            yield Streamlit.from_file(file)

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # Added by toolkit
        spec.add(ParameterSpec(("dataSetExternalId",), frozenset({"str"}), is_required=False, _is_nullable=True))
        spec.discard(ParameterSpec(("dataSetId",), frozenset({"int"}), is_required=False, _is_nullable=True))
        return spec
