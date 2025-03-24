import json
from collections.abc import Hashable, Iterable, Sequence
from functools import lru_cache
from pathlib import Path
from typing import Any, final

from cognite.client import _version as CogniteSDKVersion
from cognite.client.data_classes.capabilities import (
    Capability,
    FilesAcl,
)
from cognite.client.utils.useful_types import SequenceNotStr
from packaging.requirements import Requirement
from rich.console import Console

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
    load_yaml_inject_variables,
    safe_read,
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

    @property
    def display_name(self) -> str:
        return "Streamlit apps"

    @classmethod
    def recommended_packages(cls) -> list[Requirement]:
        return [Requirement("pyodide-http==0.2.1"), Requirement(f"cognite-sdk=={CogniteSDKVersion.__version__}")]

    def __init__(self, client: ToolkitClient, build_dir: Path | None, console: Console | None = None):
        super().__init__(client, build_dir, console)
        self._source_file_by_external_id: dict[str, Path] = {}

    @classmethod
    def get_required_capability(
        cls, items: Sequence[StreamlitWrite] | None, read_only: bool
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

    def load_resource_file(
        self, filepath: Path, environment_variables: dict[str, str | None] | None = None
    ) -> list[dict[str, Any]]:
        raw_yaml = load_yaml_inject_variables(
            self.safe_read(filepath),
            environment_variables or {},
            original_filepath=filepath,
        )
        raw_list = raw_yaml if isinstance(raw_yaml, list) else [raw_yaml]
        for item in raw_list:
            item_id = self.get_id(item)
            self._source_file_by_external_id[item_id] = filepath
            content = self._as_json_string(item_id, item["entrypoint"])
            item["cogniteToolkitAppHash"] = calculate_str_or_file_hash(content, shorten=True)
        return raw_list

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> StreamlitWrite:
        if ds_external_id := resource.pop("dataSetExternalId", None):
            resource["dataSetId"] = self.client.lookup.data_sets.id(ds_external_id, is_dry_run)
        return StreamlitWrite._load(resource)

    def dump_resource(self, resource: Streamlit, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_write().dump()
        local = local or {}
        if data_set_id := dumped.pop("dataSetId", None):
            dumped["dataSetExternalId"] = self.client.lookup.data_sets.external_id(data_set_id)
        if dumped.get("theme") == "Light" and "theme" not in local:
            dumped.pop("theme")
        return dumped

    @lru_cache
    def _as_json_string(self, external_id: str, entrypoint: str) -> str:
        source_file = self._source_file_by_external_id[external_id]
        app_path = source_file.with_name(external_id)
        if not app_path.exists():
            raise ToolkitNotADirectoryError(f"Streamlit app folder does not exists. Expected: {app_path}")
        requirements_txt = app_path / "requirements.txt"

        if requirements_txt.exists():
            requirements_txt_lines = safe_read(requirements_txt).splitlines()
        else:
            requirements_txt_lines = [str(r) for r in self.recommended_packages()]
        files = {
            py_file.relative_to(app_path).as_posix(): {"content": {"text": safe_read(py_file), "$case": "text"}}
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

    @staticmethod
    def _missing_recommended_requirements(requirements: list[str]) -> list[str]:
        missing = []
        user_requirements = {Requirement(req).name for req in requirements}
        for recommended in StreamlitLoader.recommended_packages():
            if recommended.name not in user_requirements:
                missing.append(recommended.name)
        return missing

    def create(self, items: StreamlitWriteList) -> StreamlitList:
        created = StreamlitList([])
        for item in items:
            content = self._as_json_string(item.external_id, item.entrypoint)
            to_create = item.as_file()
            created_file, _ = self.client.files.create(to_create)

            self.client.files.upload_content_bytes(content, item.external_id)
            created.append(Streamlit.from_file(created_file))
        return created

    def retrieve(self, ids: SequenceNotStr[str]) -> StreamlitList:
        files = self.client.files.retrieve_multiple(external_ids=ids, ignore_unknown_ids=True)
        return StreamlitList([Streamlit.from_file(file) for file in files])

    def update(self, items: StreamlitWriteList) -> StreamlitList:
        files = []
        for item in items:
            content = self._as_json_string(item.external_id, item.entrypoint)
            to_update = item.as_file()
            self.client.files.upload_content_bytes(content, item.external_id)
            files.append(to_update)

        updated = self.client.files.update(files, mode="replace")
        return StreamlitList([Streamlit.from_file(file) for file in updated])

    def delete(self, ids: SequenceNotStr[str]) -> int:
        self.client.files.delete(external_id=ids)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[Streamlit]:
        for file in self.client.files:
            if file.directory == "/streamlit-apps/":
                yield Streamlit.from_file(file)

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # Added by toolkit
        spec.add(ParameterSpec(("dataSetExternalId",), frozenset({"str"}), is_required=False, _is_nullable=True))
        spec.discard(ParameterSpec(("dataSetId",), frozenset({"int"}), is_required=False, _is_nullable=True))
        return spec
