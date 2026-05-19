import json
from collections.abc import Hashable, Iterable, Sequence
from pathlib import Path
from typing import Any, Literal, final

from rich.console import Console

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import AppVersionId
from cognite_toolkit._cdf_tk.client.resource_classes.app import AppRequest
from cognite_toolkit._cdf_tk.client.resource_classes.app_version import AppVersionRequest, AppVersionResponse
from cognite_toolkit._cdf_tk.client.resource_classes.group import (
    AclType,
    AllScope,
    AppHostingAcl,
    ScopeDefinition,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitRequiredValueError
from cognite_toolkit._cdf_tk.resource_ios._base_ios import FailedReadExtra, ReadExtra, ResourceIO, SuccessExtra
from cognite_toolkit._cdf_tk.utils.file import create_zip_in_memory
from cognite_toolkit._cdf_tk.utils.hashing import calculate_directory_hash
from cognite_toolkit._cdf_tk.yaml_classes import AppsYAML

from .auth import GroupAllScopedCRUD


@final
class AppIO(ResourceIO[AppVersionId, AppVersionRequest, AppVersionResponse]):
    support_drop = True
    folder_name = "apps"
    resource_cls = AppVersionResponse
    resource_write_cls = AppVersionRequest
    kind = "App"
    yaml_cls = AppsYAML
    dependencies = frozenset({GroupAllScopedCRUD})
    _doc_url = "Apps/operation/appsCreate"
    support_update = True

    def __init__(self, client: ToolkitClient, build_path: Path | None, console: Console | None):
        super().__init__(client, build_path, console)
        self.zip_path_by_version_id: dict[AppVersionId, Path] = {}

    @property
    def display_name(self) -> str:
        return "apps"

    @classmethod
    def get_minimum_scope(cls, items: Sequence[AppVersionRequest]) -> ScopeDefinition:
        return AllScope()

    @classmethod
    def create_acl(cls, actions: set[Literal["READ", "WRITE"]], scope: ScopeDefinition) -> Iterable[AclType]:
        if isinstance(scope, AllScope):
            yield AppHostingAcl(actions=sorted(actions), scope=scope)

    @classmethod
    def get_id(cls, item: AppVersionResponse | AppVersionRequest | dict[str, Any]) -> AppVersionId:
        if isinstance(item, dict):
            ext = (
                item.get("appExternalId")
                or item.get("app_external_id")
                or item.get("externalId")
                or item.get("external_id")
            )
            version = item.get("version")
            if ext is None:
                raise ToolkitRequiredValueError("App YAML must define externalId.")
            if version is None:
                raise ToolkitRequiredValueError("App YAML must define version.")
            return AppVersionId(app_external_id=ext, version=version)
        if isinstance(item, AppVersionRequest):
            return item.as_id()
        return AppVersionId(app_external_id=item.app_external_id, version=item.version)

    @classmethod
    def dump_id(cls, identifier: AppVersionId) -> dict[str, Any]:
        return identifier.dump()

    @classmethod
    def as_str(cls, identifier: AppVersionId) -> str:
        return str(identifier)

    @classmethod
    def get_dependent_items(cls, item: dict[str, Any]) -> Iterable[tuple[type[ResourceIO], Hashable]]:
        return []

    @classmethod
    def get_dependencies(cls, resource: AppsYAML) -> Iterable[tuple[type[ResourceIO], Identifier]]:
        return []

    @classmethod
    def get_extra_files(cls, filepath: Path, identifier: AppVersionId, item: dict[str, Any]) -> Iterable[ReadExtra]:
        app_external_id = identifier.app_external_id
        source_path_str = item.get("sourcePath") or item.get("source_path")
        if source_path_str is not None:
            app_root = (filepath.parent / source_path_str).resolve()
        else:
            app_root = filepath.with_name(app_external_id)

        if not app_root.is_dir():
            yield FailedReadExtra(
                code="MISSING",
                error=(
                    f"App directory not found for externalId {app_external_id!r}. "
                    f"Expected {app_root.as_posix()} to exist."
                ),
                source_path=app_root,
            )
            return

        entrypoint = item.get("entrypoint") or "index.html"
        dist_dir = app_root / "dist"
        if (dist_dir / entrypoint).is_file():
            source_dir = dist_dir
        elif (app_root / "src").is_dir() and (app_root / "package.json").is_file():
            yield FailedReadExtra(
                code="MISSING",
                error=(
                    f"App {app_external_id!r} looks like an unbuilt web project: "
                    f"Run `npm run build` (or your project's build command) in {app_root.as_posix()} "
                    f"before deploying with Toolkit."
                ),
                source_path=app_root,
            )
            return
        elif (app_root / entrypoint).is_file():
            source_dir = app_root
        else:
            yield FailedReadExtra(
                code="MISSING",
                error=(
                    f"Could not locate entrypoint {entrypoint!r} for app {app_external_id!r}. "
                    f"Expected {(dist_dir / entrypoint).as_posix()} or "
                    f"{(app_root / entrypoint).as_posix()} to exist. "
                    f"If your app has a build step, run it before deploying with Toolkit."
                ),
                source_path=app_root,
            )
            return

        package_json = app_root / "package.json"
        if not package_json.is_file():
            yield FailedReadExtra(
                code="MISSING",
                error=(
                    f"App {app_external_id!r} is missing package.json at {app_root.as_posix()}. "
                    f"This file is required to deploy to the App Hosting service."
                ),
                source_path=package_json,
            )
            return

        package_lock = app_root / "package-lock.json"
        if not package_lock.is_file():
            yield FailedReadExtra(
                code="MISSING",
                error=(
                    f"App {app_external_id!r} is missing package-lock.json at {app_root.as_posix()}. "
                    f"This file is required to deploy to the App Hosting service."
                ),
                source_path=package_lock,
            )
            return

        manifest_json = app_root / "manifest.json"
        if manifest_json.is_file():
            try:
                json.loads(manifest_json.read_text(encoding="utf-8"))
            except json.JSONDecodeError as error:
                yield FailedReadExtra(
                    code="SYNTAX-ERROR",
                    error=f"App {app_external_id!r} has an invalid manifest.json at {manifest_json.as_posix()}: {error}",
                    source_path=manifest_json,
                )
                return

        cognite_files = [package_json, package_lock]
        if manifest_json.is_file():
            cognite_files.append(manifest_json)
        source_hash = calculate_directory_hash(source_dir)
        zip_bytes = create_zip_in_memory(
            source_dir,
            additional_files={f".cognite/{f.name}": f for f in cognite_files},
        )
        yield SuccessExtra(
            source_path=source_dir,
            source_hash=source_hash,
            suffix=".zip",
            byte_content=zip_bytes,
            description="app bundle",
        )

    def load_resource_file(
        self, filepath: Path, environment_variables: dict[str, str | None] | None = None
    ) -> list[dict[str, Any]]:
        if filepath.parent.name != self.folder_name:
            return []

        raw_list = super().load_resource_file(filepath, environment_variables)
        for item in raw_list:
            app_external_id = item.get("externalId") or item.get("external_id")
            if not app_external_id:
                raise ToolkitRequiredValueError("App YAML must define externalId.")
            version = item.get("version")
            if not version:
                raise ToolkitRequiredValueError("App YAML must define version.")
            filestem = filepath.stem.rsplit(".", 1)[0]
            version_id = AppVersionId(app_external_id=app_external_id, version=version)
            self.zip_path_by_version_id[version_id] = filepath.parent / f"{filestem}.zip"

        return raw_list

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> AppVersionRequest:
        return AppVersionRequest.model_validate(resource)

    def dump_resource(self, resource: AppVersionResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        local = local or {}
        dumped: dict[str, Any] = {
            "externalId": resource.app_external_id,
            "version": resource.version,
            "lifecycleState": resource.lifecycle_state,
            "entrypoint": resource.entrypoint,
        }
        if resource.alias is not None:
            dumped["alias"] = resource.alias
        # name and description are app-level and immutable post-create; always use local values to suppress stale diff.
        for immutable_key in ("name", "description"):
            if immutable_key in local:
                dumped[immutable_key] = local[immutable_key]
        for local_only_key in ("sourcePath", "source_path"):
            if local_only_key in local:
                dumped[local_only_key] = local[local_only_key]
        return dumped

    def _deploy(self, item: AppVersionRequest) -> AppVersionResponse:
        version_id = item.as_id()
        zip_path = self.zip_path_by_version_id.get(version_id)
        if zip_path is None or not zip_path.exists():
            raise ToolkitRequiredValueError(
                f"App zip not found for {item.external_id!r} version {item.version!r}. Ensure build was run first."
            )
        try:
            self.client.tool.apps.create(
                [AppRequest(external_id=item.external_id, name=item.name, description=item.description)]
            )
        except ToolkitAPIError as error:
            if error.code != 409:
                raise
        zip_bytes = zip_path.read_bytes()
        try:
            self.client.tool.apps.versions.upload(
                external_id=item.external_id,
                version=item.version,
                entrypoint=item.entrypoint,
                zip_bytes=zip_bytes,
            )
        except ToolkitAPIError as error:
            if error.code != 409:
                raise
        update: dict[str, Any] = {"lifecycleState": {"set": item.lifecycle_state}}
        if "alias" in item.model_fields_set:
            update["alias"] = {"setNull": True} if item.alias is None else {"set": item.alias}
        self.client.tool.apps.versions.update(item.external_id, item.version, update)

        return AppVersionResponse(
            app_external_id=item.external_id,
            version=item.version,
            lifecycle_state=item.lifecycle_state,
            alias=item.alias,
            entrypoint=item.entrypoint,
        )

    def create(self, items: Sequence[AppVersionRequest]) -> list[AppVersionResponse]:
        return [self._deploy(item) for item in items]

    def update(self, items: Sequence[AppVersionRequest]) -> list[AppVersionResponse]:
        return [self._deploy(item) for item in items]

    def retrieve(self, ids: Sequence[AppVersionId]) -> list[AppVersionResponse]:
        return self.client.tool.apps.versions.retrieve(list(ids), ignore_unknown_ids=True)

    def delete(self, ids: Sequence[AppVersionId]) -> int:
        if not ids:
            return 0
        self.client.tool.apps.versions.delete(ids)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[AppVersionResponse]:
        for page in self.client.tool.apps.versions.iterate():
            yield from page
