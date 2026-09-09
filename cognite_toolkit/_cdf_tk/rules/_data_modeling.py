from collections.abc import Iterable
from pathlib import Path
from typing import Any, ClassVar

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.identifiers import ContainerId, DataModelId, ViewId
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import ResourceType
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuiltResource
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import (
    ConsistencyError,
    InternalValidatorException,
)
from cognite_toolkit._cdf_tk.constants import URL
from cognite_toolkit._cdf_tk.resource_ios import ContainerCRUD, DataModelIO, ResourceIO, ViewIO
from cognite_toolkit._cdf_tk.rules._base import RuleSetStatus, ToolkitGlobalRuleSet
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.file import format_insight_source_file

LocalResources = dict[Identifier, tuple[BuiltResource, dict[str, Any]]]


class DataModelingChangeRuleSet(ToolkitGlobalRuleSet):
    """Reports local data modeling changes that CDF will silently drop on deploy.

    Containers cannot have properties removed, and views and data models are immutable per version.
    Today this is only discovered during (or after) ``cdf deploy``. This rule set surfaces the same
    findings during ``cdf build``.
    """

    CODE_PREFIX = "DMS"
    DISPLAY_NAME = "Data modeling change checks"
    INVALID_OPERATION_CODE: ClassVar[str] = f"{CODE_PREFIX}-INVALID-OPERATION"

    def get_status(self) -> RuleSetStatus:
        if not self.client:
            return RuleSetStatus(
                code="skip",
                message=(
                    "Detecting data modeling changes that CDF cannot apply requires comparing against the "
                    "deployed containers, views and data models. Provide client credentials to enable this check."
                ),
            )
        return RuleSetStatus(
            code="ready",
            message="Will validate that local container, view and data model changes can be applied in CDF.",
        )

    def validate(self) -> Iterable[ConsistencyError | InternalValidatorException]:
        if self.client is None:
            return
        cruds: tuple[ResourceIO[Any, Any, Any], ...] = (
            ContainerCRUD(self.client, None, None),
            ViewIO(self.client, None, None),
            DataModelIO(self.client, None, None),
        )
        for crud in cruds:
            try:
                local_by_id = self._load_local_resources(crud)
                if not local_by_id:
                    continue
                cdf_items = crud.retrieve(list(local_by_id.keys()))
            except Exception as e:
                yield InternalValidatorException(
                    message=f"Failed to compare local {crud.display_name} with CDF: {e}",
                    code="INTERNAL-VALIDATOR-EXCEPTION",
                    source=crud.kind,
                )
                continue
            for cdf_item in cdf_items:
                item_id = crud.get_id(cdf_item)
                if item_id not in local_by_id:
                    continue
                resource, local_dict = local_by_id[item_id]
                cdf_dict = crud.dump_resource(cdf_item, local_dict)
                if cdf_dict == local_dict:
                    continue
                yield from self._as_insights(item_id, resource, local_dict, cdf_dict)

    def _load_local_resources(self, crud: ResourceIO[Any, Any, Any]) -> LocalResources:
        """Reload the built resources for the given CRUD so they can be compared against CDF the same
        way ``cdf deploy`` does."""
        resource_type = ResourceType(resource_folder=crud.folder_name, kind=crud.kind)
        resources_by_build_path: dict[Path, list[BuiltResource]] = {}
        for module in self.modules:
            for resource in module.resources:
                if resource.type == resource_type and resource.can_verify:
                    resources_by_build_path.setdefault(resource.build_path, []).append(resource)

        local_by_id: LocalResources = {}
        for build_path, resources in resources_by_build_path.items():
            resource_by_id = {resource.identifier: resource for resource in resources}
            for raw in crud.load_resource_file(build_path):
                request = crud.load_resource(raw)
                item_id = crud.get_id(request)
                if item_id in resource_by_id:
                    local_by_id[item_id] = (resource_by_id[item_id], request.dump())
        return local_by_id

    def _as_insights(
        self,
        item_id: Identifier,
        resource: BuiltResource,
        local_dict: dict[str, Any],
        cdf_dict: dict[str, Any],
    ) -> Iterable[ConsistencyError]:
        if isinstance(item_id, ContainerId):
            yield from self._container_insights(item_id, resource, local_dict, cdf_dict)
        elif isinstance(item_id, ViewId):
            yield from self._view_insights(item_id, resource, local_dict, cdf_dict)
        elif isinstance(item_id, DataModelId):
            yield from self._data_model_insights(item_id, resource, local_dict, cdf_dict)

    def _container_insights(
        self,
        container_id: ContainerId,
        resource: BuiltResource,
        local_dict: dict[str, Any],
        cdf_dict: dict[str, Any],
    ) -> Iterable[ConsistencyError]:
        source_file = format_insight_source_file(resource.source_path)
        has_removal = False
        for field_name in ("properties", "constraints", "indexes"):
            only_in_cdf = sorted(set(cdf_dict.get(field_name) or {}) - set(local_dict.get(field_name) or {}))
            if not only_in_cdf:
                continue
            has_removal = True
            yield ConsistencyError(
                code=self.INVALID_OPERATION_CODE,
                message=(
                    f"Local config for container {container_id} is missing {field_name} "
                    f"{humanize_collection([f'{name!r}' for name in only_in_cdf])} that are deployed in CDF. "
                    f"CDF does not support removing them, so deploying will not remove them from the container."
                ),
                fix=(
                    f"Add the {field_name} back to your local YAML, or use 'cdf modules pull' to sync your local container config. See {URL.container_changes_docs}."
                ),
                source_file=source_file,
            )

        if not has_removal:
            yield ConsistencyError(
                code=self.INVALID_OPERATION_CODE,
                message=(
                    f"Local config for container {container_id} differs from the deployed container in CDF. Containers only "
                    f"support a limited set of changes, so deploying may not apply all of them."
                ),
                fix=(
                    f"Use 'cdf modules pull' to sync your local container config with the deployed version. See {URL.container_changes_docs}."
                ),
                source_file=source_file,
            )

    def _view_insights(
        self,
        view_id: ViewId,
        resource: BuiltResource,
        local_dict: dict[str, Any],
        cdf_dict: dict[str, Any],
    ) -> Iterable[ConsistencyError]:
        source_file = format_insight_source_file(resource.source_path)
        only_in_cdf = sorted(set(cdf_dict.get("properties") or {}) - set(local_dict.get("properties") or {}))
        if only_in_cdf:
            yield ConsistencyError(
                code=self.INVALID_OPERATION_CODE,
                message=(
                    f"Local config for view {view_id} is missing properties "
                    f"{humanize_collection([f'{name!r}' for name in only_in_cdf])} that are deployed in CDF under the "
                    f"same version. Deploying will not remove them from the view."
                ),
                fix=(
                    f"Bump the view version, add the properties back to your local YAML, or use 'cdf modules pull' to sync your local view config. See {URL.container_changes_docs}."
                ),
                source_file=source_file,
            )
        else:
            yield ConsistencyError(
                code=self.INVALID_OPERATION_CODE,
                message=(
                    f"Local config for view {view_id} differs from the deployed view in CDF under the same version. "
                    f"Views are immutable once deployed, so deploying will not apply the changes."
                ),
                fix=(
                    f"Bump the view version, or use 'cdf modules pull' to sync your local view config with the deployed version. See {URL.container_changes_docs}."
                ),
                source_file=source_file,
            )

    def _data_model_insights(
        self,
        data_model_id: DataModelId,
        resource: BuiltResource,
        local_dict: dict[str, Any],
        cdf_dict: dict[str, Any],
    ) -> Iterable[ConsistencyError]:
        source_file = format_insight_source_file(resource.source_path)
        local_views = {ViewId._load(view) for view in local_dict.get("views") or []}
        cdf_views = {ViewId._load(view) for view in cdf_dict.get("views") or []}
        local_version_by_view = {(view_id.space, view_id.external_id): view_id.version for view_id in local_views}

        removed: list[ViewId] = []
        version_changed: list[tuple[ViewId, str]] = []
        for view_id in sorted(cdf_views - local_views, key=str):
            local_version = local_version_by_view.get((view_id.space, view_id.external_id))
            if local_version is None:
                removed.append(view_id)
            else:
                version_changed.append((view_id, local_version))

        if version_changed:
            changes = humanize_collection(
                [
                    f"'{view_id.space}:{view_id.external_id}' from {view_id.version!r} to {local_version!r}"
                    for view_id, local_version in version_changed
                ]
            )
            yield ConsistencyError(
                code=self.INVALID_OPERATION_CODE,
                message=(
                    f"Local config for data model {data_model_id} changes the view version {changes}, but keeps the "
                    f"same data model version. Deploying will not apply the change."
                ),
                fix=(
                    f"Bump the data model version, or use 'cdf modules pull' to sync your local data model config with the deployed version. See {URL.container_changes_docs}."
                ),
                source_file=source_file,
            )

        if removed:
            yield ConsistencyError(
                code=self.INVALID_OPERATION_CODE,
                message=(
                    f"Local config for data model {data_model_id} is missing views "
                    f"{humanize_collection([f'{view_id!s}' for view_id in removed])} that are deployed in CDF under "
                    f"the same version. Deploying will not remove them from the data model."
                ),
                fix=(
                    f"Bump the data model version, add the views back to your local YAML, or use 'cdf modules pull' to sync your local data model config. See {URL.container_changes_docs}."
                ),
                source_file=source_file,
            )

        if not removed and not version_changed:
            yield ConsistencyError(
                code=self.INVALID_OPERATION_CODE,
                message=(
                    f"Local config for data model {data_model_id} differs from the deployed data model in CDF under "
                    f"the same version. Data models are immutable once deployed, so deploying will not apply the changes."
                ),
                fix=(
                    f"Bump the data model version, or use 'cdf modules pull' to sync your local data model config with the deployed version. See {URL.container_changes_docs}."
                ),
                source_file=source_file,
            )
