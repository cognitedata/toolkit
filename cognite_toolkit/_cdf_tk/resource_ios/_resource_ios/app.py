from collections.abc import Hashable, Iterable, Sequence
from pathlib import Path
from typing import Any, Literal, final

from rich.console import Console

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.app import AppRequest, AppResponse
from cognite_toolkit._cdf_tk.client.resource_classes.group import (
    AclType,
    AllScope,
    AppHostingAcl,
    ScopeDefinition,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitRequiredValueError
from cognite_toolkit._cdf_tk.resource_ios._base_ios import ResourceIO
from cognite_toolkit._cdf_tk.utils.file import create_temporary_zip
from cognite_toolkit._cdf_tk.yaml_classes import AppsYAML

from .auth import GroupAllScopedCRUD


@final
class AppIO(ResourceIO[ExternalId, AppRequest, AppResponse]):
    support_drop = True
    folder_name = "apps"
    resource_cls = AppResponse
    resource_write_cls = AppRequest
    kind = "App"
    yaml_cls = AppsYAML
    dependencies = frozenset({GroupAllScopedCRUD})
    _doc_url = "Apps/operation/appsCreate"
    support_update = True

    def __init__(self, client: ToolkitClient, build_path: Path | None, console: Console | None):
        super().__init__(client, build_path, console)
        self.app_dir_by_external_id: dict[str, Path] = {}

    @property
    def display_name(self) -> str:
        return "apps"

    @classmethod
    def get_minimum_scope(cls, items: Sequence[AppRequest]) -> ScopeDefinition:
        return AllScope()

    @classmethod
    def create_acl(cls, actions: set[Literal["READ", "WRITE"]], scope: ScopeDefinition) -> Iterable[AclType]:
        if isinstance(scope, AllScope):
            yield AppHostingAcl(actions=sorted(actions), scope=scope)

    @classmethod
    def get_id(cls, item: AppResponse | AppRequest | dict) -> ExternalId:
        if isinstance(item, dict):
            ext = item.get("appExternalId") or item.get("app_external_id") or item.get("externalId")
            if ext is None:
                raise ToolkitRequiredValueError("App YAML must define appExternalId.")
            return ExternalId(external_id=ext)
        if isinstance(item, AppRequest):
            return item.as_id()
        return ExternalId(external_id=item.app_external_id)

    @classmethod
    def dump_id(cls, identifier: ExternalId) -> dict[str, Any]:
        return identifier.dump()

    @classmethod
    def as_str(cls, identifier: ExternalId) -> str:
        return identifier.external_id

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceIO], Hashable]]:
        return []

    @classmethod
    def get_dependencies(cls, resource: AppsYAML) -> Iterable[tuple[type[ResourceIO], Identifier]]:
        return []

    def load_resource_file(
        self, filepath: Path, environment_variables: dict[str, str | None] | None = None
    ) -> list[dict[str, Any]]:
        if filepath.parent.name != self.folder_name:
            return []
        if self.resource_build_path is None:
            raise ValueError("build_path must be set to deploy apps.")

        raw_list = super().load_resource_file(filepath, environment_variables)
        for item in raw_list:
            app_external_id = item.get("appExternalId") or item.get("app_external_id")
            if not app_external_id:
                raise ToolkitRequiredValueError("App YAML must define appExternalId.")
            if not (item.get("versionTag") or item.get("version_tag")):
                raise ToolkitRequiredValueError("App YAML must define versionTag.")
            self.app_dir_by_external_id[app_external_id] = self.resource_build_path / app_external_id

        return raw_list

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> AppRequest:
        return AppRequest.model_validate(resource)

    def dump_resource(self, resource: AppResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        return resource.as_request_resource().dump(context="toolkit")

    def _deploy(self, item: AppRequest) -> AppResponse:
        app_dir = self.app_dir_by_external_id.get(item.app_external_id)
        if app_dir is None:
            raise ToolkitRequiredValueError(
                f"App directory not found for {item.app_external_id!r}. Ensure build was run first."
            )
        self.client.tool.apps.ensure_app(item)
        with create_temporary_zip(app_dir, "app.zip") as zip_path:
            zip_bytes = zip_path.read_bytes()
            self.client.tool.apps.upload_version(
                app_external_id=item.app_external_id,
                version_tag=item.version_tag,
                entry_path=item.entry_path,
                zip_bytes=zip_bytes,
            )
        if item.published:
            self.client.tool.apps.publish(item.app_external_id, item.version_tag)
        return AppResponse(
            app_external_id=item.app_external_id,
            version_tag=item.version_tag,
            name=item.name,
            description=item.description,
            published=item.published,
            entry_path=item.entry_path,
            lifecycle_state="PUBLISHED" if item.published else "DRAFT",
            alias="ACTIVE" if item.published else None,
        )

    def create(self, items: Sequence[AppRequest]) -> list[AppResponse]:
        if self.resource_build_path is None:
            raise ValueError("build_path must be set to deploy apps.")
        return [self._deploy(item) for item in items]

    def update(self, items: Sequence[AppRequest]) -> list[AppResponse]:
        if self.resource_build_path is None:
            raise ValueError("build_path must be set to deploy apps.")
        return [self._deploy(item) for item in items]

    def retrieve(self, ids: Sequence[ExternalId]) -> list[AppResponse]:
        return self.client.tool.apps.retrieve(list(ids))

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
        for page in self.client.tool.apps.iterate():
            yield from page
