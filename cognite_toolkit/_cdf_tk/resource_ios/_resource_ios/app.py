import io
import os
import zipfile
from collections.abc import Hashable, Iterable, Sequence
from pathlib import Path
from typing import Any, Literal, final

from rich.console import Console

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.identifiers import AppVersionId
from cognite_toolkit._cdf_tk.client.resource_classes.app import AppRequest, AppResponse
from cognite_toolkit._cdf_tk.client.resource_classes.group import (
    AclType,
    AllScope,
    AppHostingAcl,
    ScopeDefinition,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitRequiredValueError, ToolkitValueError
from cognite_toolkit._cdf_tk.resource_ios._base_ios import FailedReadExtra, ReadExtra, ResourceIO, SuccessExtra
from cognite_toolkit._cdf_tk.utils.hashing import calculate_directory_hash
from cognite_toolkit._cdf_tk.yaml_classes import AppsYAML

from .auth import GroupAllScopedCRUD

_EXCLUDE_DIRS = {"__pycache__", "node_modules", ".git"}

_LIFECYCLE_ORDER = ["DRAFT", "PUBLISHED", "DEPRECATED", "ARCHIVED"]


def _zip_app_directory(source_dir: Path) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", strict_timestamps=False) as zf:
        for root, dirs, files in os.walk(source_dir):
            dirs[:] = [d for d in dirs if d not in _EXCLUDE_DIRS]
            root_path = Path(root)
            arc_root = root_path.relative_to(source_dir)
            zf.write(root_path, arcname=str(arc_root))
            for filename in files:
                file_path = root_path / filename
                zf.write(file_path, arcname=str(file_path.relative_to(source_dir)))
    return buffer.getvalue()


@final
class AppIO(ResourceIO[AppVersionId, AppRequest, AppResponse]):
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
        self.zip_path_by_version_id: dict[AppVersionId, Path] = {}

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
    def get_id(cls, item: AppResponse | AppRequest | dict) -> AppVersionId:
        if isinstance(item, dict):
            ext = item.get("externalId") or item.get("external_id")
            version = item.get("version")
            if ext is None:
                raise ToolkitRequiredValueError("App YAML must define externalId.")
            if version is None:
                raise ToolkitRequiredValueError("App YAML must define version.")
            return AppVersionId(external_id=ext, version=version)
        if isinstance(item, AppRequest):
            return item.as_id()
        return AppVersionId(external_id=item.external_id, version=item.version)

    @classmethod
    def dump_id(cls, identifier: AppVersionId) -> dict[str, Any]:
        return identifier.dump()

    @classmethod
    def as_str(cls, identifier: AppVersionId) -> str:
        return str(identifier)

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceIO], Hashable]]:
        return []

    @classmethod
    def get_dependencies(cls, resource: AppsYAML) -> Iterable[tuple[type[ResourceIO], Identifier]]:
        return []

    @classmethod
    def get_extra_files(cls, filepath: Path, identifier: AppVersionId, item: dict[str, Any]) -> Iterable[ReadExtra]:
        app_external_id = identifier.external_id
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

        source_dir = app_root / "dist" if (app_root / "dist").is_dir() else app_root
        source_hash = calculate_directory_hash(source_dir)
        zip_bytes = _zip_app_directory(source_dir)
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
            version_id = AppVersionId(external_id=app_external_id, version=version)
            self.zip_path_by_version_id[version_id] = filepath.parent / f"{filestem}.zip"

        return raw_list

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> AppRequest:
        return AppRequest.model_validate(resource)

    def dump_resource(self, resource: AppResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_request_resource().dump(context="toolkit")
        local = local or {}
        # name and description are immutable in CDF post-create; always use local values to suppress stale diff.
        for immutable_key in ("name", "description"):
            if immutable_key in local:
                dumped[immutable_key] = local[immutable_key]
        for local_only_key in ("sourcePath", "source_path"):
            if local_only_key in local:
                dumped[local_only_key] = local[local_only_key]
        return dumped

    def _deploy(self, item: AppRequest) -> AppResponse:
        version_id = item.as_id()
        zip_path = self.zip_path_by_version_id.get(version_id)
        if zip_path is None or not zip_path.exists():
            raise ToolkitRequiredValueError(
                f"App zip not found for {item.external_id!r} version {item.version!r}. Ensure build was run first."
            )
        self.client.tool.apps.ensure_app(item)
        zip_bytes = zip_path.read_bytes()
        self.client.tool.apps.upload_version(
            external_id=item.external_id,
            version=item.version,
            entrypoint=item.entrypoint,
            zip_bytes=zip_bytes,
        )

        current = self.client.tool.apps.retrieve_version(item.external_id, item.version, ignore_unknown_ids=True)
        current_lifecycle = current.lifecycle_state if current else "DRAFT"
        current_alias = current.alias if current else None

        if item.lifecycle_state != current_lifecycle:
            current_idx = _LIFECYCLE_ORDER.index(current_lifecycle) if current_lifecycle in _LIFECYCLE_ORDER else 0
            target_idx = _LIFECYCLE_ORDER.index(item.lifecycle_state) if item.lifecycle_state in _LIFECYCLE_ORDER else 0
            if target_idx < current_idx:
                raise ToolkitValueError(
                    f"Cannot transition app {item.external_id!r} version {item.version!r} "
                    f"from {current_lifecycle!r} to {item.lifecycle_state!r}: lifecycle transitions are forward-only."
                )
            self.client.tool.apps.transition_lifecycle(item.external_id, item.version, item.lifecycle_state)

        alias_explicitly_set = "alias" in item.model_fields_set
        if alias_explicitly_set and item.alias != current_alias:
            if item.alias is not None and item.lifecycle_state not in ("PUBLISHED",):
                raise ToolkitValueError(
                    f"Cannot set alias {item.alias!r} on app {item.external_id!r} version {item.version!r}: "
                    f"aliases are only valid on PUBLISHED versions (current lifecycle: {item.lifecycle_state!r})."
                )
            self.client.tool.apps.set_alias(item.external_id, item.version, item.alias)

        return AppResponse(
            external_id=item.external_id,
            version=item.version,
            name=item.name,
            description=item.description,
            lifecycle_state=item.lifecycle_state,
            alias=item.alias,
            entrypoint=item.entrypoint,
        )

    def create(self, items: Sequence[AppRequest]) -> list[AppResponse]:
        return [self._deploy(item) for item in items]

    def update(self, items: Sequence[AppRequest]) -> list[AppResponse]:
        return [self._deploy(item) for item in items]

    def retrieve(self, ids: Sequence[AppVersionId]) -> list[AppResponse]:
        results: list[AppResponse] = []
        for version_id in ids:
            response = self.client.tool.apps.retrieve_version(
                version_id.external_id, version_id.version, ignore_unknown_ids=True
            )
            if response is not None:
                results.append(response)
        return results

    def delete(self, ids: Sequence[AppVersionId]) -> int:
        if not ids:
            return 0
        by_app: dict[str, list[AppVersionId]] = {}
        for version_id in ids:
            by_app.setdefault(version_id.external_id, []).append(version_id)
        for app_external_id, version_ids in by_app.items():
            self.client.tool.apps.delete_version(app_external_id, version_ids)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[AppResponse]:
        for page in self.client.tool.apps.iterate():
            yield from page
