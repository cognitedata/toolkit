from collections.abc import Hashable, Iterable, Sequence
from pathlib import Path
from typing import Any, Literal, final

from rich.console import Console

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.http_client import RequestMessage
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.request_classes.filters import DuneAppFilter
from cognite_toolkit._cdf_tk.client.resource_classes.app import AppRequest, AppResponse
from cognite_toolkit._cdf_tk.client.resource_classes.group import (
    AclType,
    AllScope,
    DataSetScope,
    FilesAcl,
    ScopeDefinition,
)
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceCRUD
from cognite_toolkit._cdf_tk.exceptions import ToolkitRequiredValueError
from cognite_toolkit._cdf_tk.utils import calculate_directory_hash, calculate_hash
from cognite_toolkit._cdf_tk.utils.acl_helper import dataset_scoped_resource
from cognite_toolkit._cdf_tk.utils.file import create_temporary_zip
from cognite_toolkit._cdf_tk.yaml_classes import AppsYAML

from .auth import GroupAllScopedCRUD
from .data_organization import DataSetsCRUD


@final
class AppCRUD(ResourceCRUD[ExternalId, AppRequest, AppResponse]):
    support_drop = True
    folder_name = "apps"
    resource_cls = AppResponse
    resource_write_cls = AppRequest
    kind = "App"
    yaml_cls = AppsYAML
    dependencies = frozenset({DataSetsCRUD, GroupAllScopedCRUD})
    _doc_url = "Files/operation/initFileUpload"
    metadata_value_limit = 512
    support_update = True
    _toolkit_hash_key = "cdf-toolkit-app-hash"

    def __init__(self, client: ToolkitClient, build_path: Path | None, console: Console | None):
        super().__init__(client, build_path, console)
        self.data_set_id_by_file_external_id: dict[str, int] = {}
        self.app_dir_by_file_external_id: dict[str, Path] = {}

    @property
    def display_name(self) -> str:
        return "apps"

    @classmethod
    def get_minimum_scope(cls, items: Sequence[AppRequest]) -> ScopeDefinition:
        return dataset_scoped_resource(items)

    @classmethod
    def create_acl(cls, actions: set[Literal["READ", "WRITE"]], scope: ScopeDefinition) -> Iterable[AclType]:
        if isinstance(scope, AllScope | DataSetScope):
            yield FilesAcl(actions=sorted(actions), scope=scope)

    @classmethod
    def get_id(cls, item: AppResponse | AppRequest | dict) -> ExternalId:
        if isinstance(item, dict):
            if "appExternalId" in item and "version" in item:
                return ExternalId(external_id=f"{item['appExternalId']}-{item['version']}")
            ext = item.get("externalId")
            if ext is None:
                raise ToolkitRequiredValueError("App must have appExternalId and version, or externalId set.")
            return ExternalId(external_id=ext)
        if isinstance(item, AppRequest):
            return item.as_id()
        if item.external_id is None:
            raise ToolkitRequiredValueError("App must have external_id set.")
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

    @classmethod
    def get_dependencies(cls, resource: AppsYAML) -> Iterable[tuple[type[ResourceCRUD], Identifier]]:
        if resource.data_set_external_id:
            yield DataSetsCRUD, ExternalId(external_id=resource.data_set_external_id)

    def load_resource_file(
        self, filepath: Path, environment_variables: dict[str, str | None] | None = None
    ) -> list[dict[str, Any]]:
        if filepath.parent.name != self.folder_name:
            return []
        if self.resource_build_path is None:
            raise ValueError("build_path must be set to compare apps as app code must be compared.")

        raw_list = super().load_resource_file(filepath, environment_variables)
        for item in raw_list:
            if "appExternalId" not in item or "version" not in item:
                raise ToolkitRequiredValueError(
                    "App YAML must define appExternalId and version (file external id is '<appExternalId>-<version>')."
                )
            file_external_id = f"{item['appExternalId']}-{item['version']}"
            app_rootdir = Path(self.resource_build_path / item["appExternalId"])
            self.app_dir_by_file_external_id[file_external_id] = app_rootdir
            item[self._toolkit_hash_key] = self._create_hash_values(app_rootdir)

        return raw_list

    @classmethod
    def _create_hash_values(cls, app_rootdir: Path) -> str:
        root_hash = calculate_directory_hash(
            app_rootdir, exclude_prefixes={".DS_Store"}, ignore_files={".pyc"}, shorten=True
        )
        hash_value = f"/={root_hash}"
        to_search = [app_rootdir]
        while to_search:
            search_dir = to_search.pop()
            for file in sorted(search_dir.glob("*"), key=lambda x: x.relative_to(app_rootdir).as_posix()):
                if file.is_dir():
                    to_search.append(file)
                    continue
                elif file.is_file() and file.suffix == ".pyc":
                    continue
                elif file.is_file() and file.name == ".DS_Store":
                    continue
                file_hash = calculate_hash(file, shorten=True)
                new_entry = f"{file.relative_to(app_rootdir).as_posix()}={file_hash}"
                if len(hash_value) + len(new_entry) > (cls.metadata_value_limit - 1):
                    break
                hash_value += f";{new_entry}"
        return hash_value

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> AppRequest:
        item_id = self.get_id(resource)
        file_external_id = item_id.external_id
        if ds_external_id := resource.pop("dataSetExternalId", None):
            data_set_id = self.client.lookup.data_sets.id(ds_external_id, is_dry_run)
            self.data_set_id_by_file_external_id[file_external_id] = data_set_id
            resource["dataSetId"] = data_set_id
        resource.pop("metadata", None)
        return AppRequest.model_validate(resource)

    def dump_resource(self, resource: AppResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_request_resource().dump(context="toolkit")
        local = local or {}
        if data_set_id := dumped.pop("dataSetId", None):
            dumped["dataSetExternalId"] = self.client.lookup.data_sets.external_id(data_set_id)
        return dumped

    def _upload_zip(self, upload_url: str, content: bytes) -> None:
        request = RequestMessage(
            endpoint_url=upload_url,
            method="PUT",
            content_type="application/zip",
            data_content=content,
        )
        result = self.client.http_client.request_single_retries(request)
        result.get_success_or_raise(request)

    def _deploy_zip(self, item: AppRequest, *, overwrite: bool) -> AppResponse:
        feid = item.external_id
        app_rootdir = self.app_dir_by_file_external_id[feid]
        with create_temporary_zip(app_rootdir, "app.zip") as zip_path:
            zip_bytes = zip_path.read_bytes()
            responses = self.client.tool.apps.create([item], overwrite=overwrite)
            if not responses:
                raise ToolkitRequiredValueError(f"No response when creating app file metadata for {feid!r}.")
            response = responses[0]
            if not response.upload_url:
                raise ToolkitRequiredValueError("Files init response missing upload_url for zip upload.")
            self._upload_zip(response.upload_url, zip_bytes)
            return response

    def create(self, items: Sequence[AppRequest]) -> list[AppResponse]:
        if self.resource_build_path is None:
            raise ValueError("build_path must be set to deploy apps (zip is built from the app directory).")
        return [self._deploy_zip(item, overwrite=False) for item in items]

    def update(self, items: Sequence[AppRequest]) -> list[AppResponse]:
        if self.resource_build_path is None:
            raise ValueError("build_path must be set to update apps (zip is built from the app directory).")
        return [self._deploy_zip(item, overwrite=True) for item in items]

    def retrieve(self, ids: Sequence[ExternalId]) -> list[AppResponse]:
        return self.client.tool.apps.retrieve(list(ids), ignore_unknown_ids=True)

    def delete(self, ids: Sequence[ExternalId]) -> int:
        if not ids:
            return 0
        self.client.tool.apps.delete(list(ids), ignore_unknown_ids=True)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[AppResponse]:
        ds_filter: DuneAppFilter | None = None
        if data_set_external_id is not None:
            ds_id = self.client.lookup.data_sets.id(data_set_external_id)
            ds_filter = DuneAppFilter.from_asset_subtree_and_data_sets(data_set_id=ds_id)
        for page in self.client.tool.apps.iterate(filter=ds_filter, limit=None):
            yield from page
