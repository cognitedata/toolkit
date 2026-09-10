from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import Any, ClassVar

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.identifiers import ContainerId, DataModelId, ViewId
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import ResourceType
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuiltResource
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import ConsistencyError, Insight
from cognite_toolkit._cdf_tk.constants import URL
from cognite_toolkit._cdf_tk.resource_ios import ContainerCRUD, DataModelIO, ResourceIO, ViewIO
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.file import format_insight_source_file, relative_to_if_possible

from ._base import InternalValidatorException, RuleSetStatus, ToolkitGlobalRuleSet

LocalResources = dict[Identifier, tuple[BuiltResource, dict[str, Any]]]


class DependencyRuleSet(ToolkitGlobalRuleSet):
    """Validates that resources reference each other correctly, and that local data modeling changes
    can actually be applied by CDF.

    Both checks are grouped under the same table row because they share the same shape: they require a
    client to compare the local modules against the state already deployed in CDF.
    """

    DISPLAY_NAME = "Dependency checks"
    INVALID_OPERATION_CODE: ClassVar[str] = "DMS-INVALID-OPERATION"

    def get_status(self) -> RuleSetStatus:
        if self.client is None:
            message = "No client provided, will only validate dependencies between resources within the provided modules, but not validate against CDF."
        else:
            message = "Will validate dependencies and data modeling changes against CDF."
        return RuleSetStatus(code="ready", message=message)

    def validate(self) -> Iterable[Insight | InternalValidatorException]:
        yield from self._validate_dependencies()
        if self.client is not None:
            yield from self._validate_data_modeling_changes()

    def _validate_dependencies(self) -> Iterable[Insight]:
        """CDF dependency validations are validations that require checking the existence of resources in CDF."""
        built_resource_ids: set[tuple[type[ResourceIO], Identifier]] = {
            (resource.crud_cls, resource.identifier) for module in self.modules for resource in module.resources
        }
        missing_locally_by_crud_cls: dict[type[ResourceIO], dict[Identifier, list[BuiltResource]]] = defaultdict(
            lambda: defaultdict(list)
        )
        for module in self.modules:
            for resource in module.resources:
                for crud_cls, dependency_id in resource.dependencies:
                    if (crud_cls, dependency_id) not in built_resource_ids:
                        missing_locally_by_crud_cls[crud_cls][dependency_id].append(resource)

        if self.client:
            for crud_cls, expected_by_identifier in missing_locally_by_crud_cls.items():
                crud = crud_cls(self.client, None, None)
                display_name = crud.display_name
                existing_in_cdf = {
                    crud.get_id(cdf_item) for cdf_item in crud.retrieve(list(expected_by_identifier.keys()))
                }
                if missing := set(expected_by_identifier.keys()) - existing_in_cdf:
                    for identifier in missing:
                        referencing_resources = expected_by_identifier[identifier]
                        yield ConsistencyError(
                            code="UNKNOWN-REFERENCE",
                            message=f"Unknown reference to {display_name} with id '{identifier}'",
                            fix=f"Ensure that {display_name} exists or remove the reference to it.",
                            source_file=self._source_files_for_resources(referencing_resources),
                        )
        else:
            for crud_cls, expected_by_identifier in missing_locally_by_crud_cls.items():
                resource_type_name = f"{crud_cls.kind.lower()} ({crud_cls.folder_name})"
                for identifier, expected_resources in expected_by_identifier.items():
                    referenced_str = self._create_reference_string(expected_resources)
                    yield ConsistencyError(
                        code="UNVERIFIED-REFERENCE",
                        message=f"Missing {resource_type_name} '{identifier}'. It is referenced by {referenced_str}.",
                        fix=f"Provide credentials to enable CDF verification. "
                        f"Or ensure that {resource_type_name} exists or remove the reference to it.",
                        source_file=self._source_files_for_resources(expected_resources),
                    )

    def _source_files_for_resources(self, resources: list[BuiltResource]) -> str:
        unique_paths = list(dict.fromkeys(format_insight_source_file(resource.source_path) for resource in resources))
        return ", ".join(unique_paths)

    def _create_reference_string(self, expected_resources: list[BuiltResource]) -> str:
        return " - ".join(
            f"{resource.identifier!s} in {relative_to_if_possible(resource.source_path).as_posix()!r}"
            for resource in expected_resources
        )

    def _validate_data_modeling_changes(self) -> Iterable[ConsistencyError | InternalValidatorException]:
        """Reports local container, view and data model changes that CDF will silently drop on deploy.

        Containers cannot have properties removed, and views and data models are immutable per version.
        Today this is only discovered during (or after) ``cdf deploy``. This surfaces the same findings
        during ``cdf build``.
        """
        assert self.client is not None
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
                yield from self._as_data_modeling_insights(item_id, resource, local_dict, cdf_dict)

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

    def _as_data_modeling_insights(
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
