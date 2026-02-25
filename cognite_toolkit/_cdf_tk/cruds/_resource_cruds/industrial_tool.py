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
from packaging.requirements import Requirement
from rich.console import Console

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import RequestMessage
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.streamlit_ import StreamlitRequest, StreamlitResponse
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceCRUD
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotADirectoryError, ToolkitRequiredValueError
from cognite_toolkit._cdf_tk.resource_classes import StreamlitYAML
from cognite_toolkit._cdf_tk.utils import (
    load_yaml_inject_variables,
    safe_read,
)
from cognite_toolkit._cdf_tk.utils.hashing import calculate_hash

from .auth import GroupAllScopedCRUD
from .data_organization import DataSetsCRUD


@final
class StreamlitCRUD(ResourceCRUD[ExternalId, StreamlitRequest, StreamlitResponse]):
    folder_name = "streamlit"
    resource_cls = StreamlitResponse
    resource_write_cls = StreamlitRequest
    kind = "Streamlit"
    dependencies = frozenset({DataSetsCRUD, GroupAllScopedCRUD})
    _doc_url = "Files/operation/initFileUpload"
    _metadata_hash_key = "cdf-toolkit-app-hash"
    yaml_cls = StreamlitYAML

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
        cls, items: Sequence[StreamlitRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []

        actions = [FilesAcl.Action.Read] if read_only else [FilesAcl.Action.Read, FilesAcl.Action.Write]

        scope: FilesAcl.Scope.All | FilesAcl.Scope.DataSet = FilesAcl.Scope.All()  # type: ignore[valid-type]
        if items:
            if data_set_ids := {item.data_set_id for item in items if item.data_set_id}:
                scope = FilesAcl.Scope.DataSet(list(data_set_ids))

        return FilesAcl(actions, scope)

    @classmethod
    def get_id(cls, item: StreamlitRequest | StreamlitResponse | dict) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        if item.external_id is None:
            raise ToolkitRequiredValueError("Streamlit app must have external_id set.")
        return ExternalId(external_id=item.external_id)

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def as_str(cls, id: ExternalId) -> str:
        return id.external_id

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceCRUD], Hashable]]:
        if "dataSetExternalId" in item:
            yield DataSetsCRUD, ExternalId(external_id=item["dataSetExternalId"])

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
            external_id = self.get_id(item).external_id
            self._source_file_by_external_id[external_id] = filepath
            content = self._as_json_string(external_id, item["entrypoint"])
            item[self._metadata_hash_key] = calculate_hash(content, shorten=True)
        return raw_list

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> StreamlitRequest:
        if ds_external_id := resource.pop("dataSetExternalId", None):
            resource["dataSetId"] = self.client.lookup.data_sets.id(ds_external_id, is_dry_run)
        return StreamlitRequest.model_validate(resource)

    def dump_resource(self, resource: StreamlitResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_request_resource().dump(context="toolkit")
        local = local or {}
        if data_set_id := dumped.pop("dataSetId", None):
            dumped["dataSetExternalId"] = self.client.lookup.data_sets.external_id(data_set_id)
        if dumped.get("theme") == "Light" and "theme" not in local:
            # Removing the default
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
        for recommended in StreamlitCRUD.recommended_packages():
            if recommended.name not in user_requirements:
                missing.append(recommended.name)
        return missing

    def _upload_content(self, upload_url: str, content: str) -> None:
        result = self.client.http_client.request_single_retries(
            RequestMessage(
                endpoint_url=upload_url,
                method="PUT",
                content_type="application/json",
                data_content=content.encode("utf-8"),
            )
        )
        result.get_success_or_raise()

    def create(self, items: Sequence[StreamlitRequest]) -> list[StreamlitResponse]:
        created: list[StreamlitResponse] = []
        for item in items:
            content = self._as_json_string(item.external_id, item.entrypoint)
            responses = self.client.tool.streamlit.create([item])
            for response in responses:
                if not response.upload_url:
                    raise ToolkitRequiredValueError("Create response missing upload_url.")
                self._upload_content(response.upload_url, content)
                created.append(response)
        return created

    def retrieve(self, ids: Sequence[ExternalId]) -> list[StreamlitResponse]:
        return self.client.tool.streamlit.retrieve(list(ids), ignore_unknown_ids=True)

    def update(self, items: Sequence[StreamlitRequest]) -> list[StreamlitResponse]:
        updated: list[StreamlitResponse] = []
        for item in items:
            content = self._as_json_string(item.external_id, item.entrypoint)
            responses = self.client.tool.streamlit.create([item], overwrite=True)
            for response in responses:
                if not response.upload_url:
                    raise ToolkitRequiredValueError("Create response missing upload_url for content upload.")
                self._upload_content(response.upload_url, content)
                updated.append(response)
        return updated

    def delete(self, ids: Sequence[ExternalId]) -> int:
        if not ids:
            return 0
        self.client.tool.streamlit.delete(list(ids))
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[StreamlitResponse]:
        for page in self.client.tool.streamlit.iterate(limit=None):
            yield from page
