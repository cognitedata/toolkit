from collections import defaultdict
from collections.abc import Iterable
from typing import Any, ClassVar

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.identifiers import ContainerId, DataModelId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    ContainerPropertyDefinition,
    ContainerRequest,
    ContainerResponse,
    DataModelRequest,
    DataModelResponse,
    ViewCorePropertyRequest,
    ViewRequest,
    ViewRequestProperty,
    ViewResponse,
)
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuiltResource
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import ConsistencyError, Insight
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._types import AbsoluteFilePath
from cognite_toolkit._cdf_tk.constants import URL
from cognite_toolkit._cdf_tk.resource_ios import ContainerCRUD, DataModelIO, ResourceIO, ViewIO
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.file import relative_to_if_possible

from ._base import InternalValidatorException, RuleSetStatus, ToolkitGlobalRuleSet


class DependencyRuleSet(ToolkitGlobalRuleSet):
    """
    Validates that resources reference each other correctly, and that local state changes
    can actually be applied in CDF.
    """

    DISPLAY_NAME = "Dependency checks"
    INVALID_OPERATION_CODE: ClassVar[str] = "INVALID-OPERATION"

    def get_status(self) -> RuleSetStatus:
        if self.client is None:
            message = "No client provided, will only validate dependencies between resources within the provided modules, but not validate against CDF."
        else:
            message = "Will validate dependencies and state changes against CDF."
        return RuleSetStatus(code="ready", message=message)

    def validate(self) -> Iterable[Insight | InternalValidatorException]:
        yield from self._validate_dependencies()
        if self.client is not None:
            yield from self._validate_data_modeling_changes(self.client)

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
                resource_label = crud_cls.kind.lower()
                existing_in_cdf = {
                    crud.get_id(cdf_item) for cdf_item in crud.retrieve(list(expected_by_identifier.keys()))
                }
                if missing := set(expected_by_identifier.keys()) - existing_in_cdf:
                    for identifier in missing:
                        referencing_resources = expected_by_identifier[identifier]
                        yield ConsistencyError(
                            code="UNKNOWN-REFERENCE",
                            message=f"Unknown reference to {resource_label} with id '{identifier}'",
                            fix=f"Ensure that the {resource_label} exists or remove the reference to it.",
                            source_files=[resource.source_path for resource in referencing_resources],
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
                        source_files=[resource.source_path for resource in expected_resources],
                    )

    def _create_reference_string(self, expected_resources: list[BuiltResource]) -> str:
        return " - ".join(
            f"{resource.identifier!s} in {relative_to_if_possible(resource.source_path).as_posix()!r}"
            for resource in expected_resources
        )

    def _validate_data_modeling_changes(
        self, client: ToolkitClient
    ) -> Iterable[ConsistencyError | InternalValidatorException]:
        """Reports local container, view and data model changes that CDF will silently drop on deploy.

        Containers cannot have properties removed, and views and data models are immutable per version.
        Today this is only discovered during (or after) ``cdf deploy``. This surfaces the same findings
        during ``cdf build``.
        """
        cruds: tuple[ResourceIO[Any, Any, Any, Any], ...] = (
            ContainerCRUD(client, None, None),
            ViewIO(client, None, None),
            DataModelIO(client, None, None),
        )
        for crud in cruds:
            try:
                local_by_id: dict[Identifier, tuple[BuiltResource, Any]] = {}
                for module in self.modules:
                    local_by_id.update(module.load_local_resources(crud))
                if not local_by_id:
                    continue
                cdf_items = crud.retrieve(list(local_by_id.keys()))
            except Exception as e:
                yield InternalValidatorException(
                    message=f"Failed to compare local {crud.display_name} with CDF: {e}",
                    source=crud.kind,
                )
                continue
            for cdf_item in cdf_items:
                item_id = crud.get_id(cdf_item)
                if item_id not in local_by_id:
                    continue
                resource, request = local_by_id[item_id]
                yield from self._as_data_modeling_insights(item_id, resource, request, cdf_item)

    def _as_data_modeling_insights(
        self,
        item_id: Identifier,
        resource: BuiltResource,
        request: Any,
        cdf_item: Any,
    ) -> Iterable[ConsistencyError]:
        if isinstance(item_id, ContainerId):
            assert isinstance(request, ContainerRequest) and isinstance(cdf_item, ContainerResponse)
            yield from self._container_insights(item_id, resource.source_path, request, cdf_item)
        elif isinstance(item_id, ViewId):
            assert isinstance(request, ViewRequest) and isinstance(cdf_item, ViewResponse)
            yield from self._view_insights(item_id, resource.source_path, request, cdf_item)
        elif isinstance(item_id, DataModelId):
            assert isinstance(request, DataModelRequest) and isinstance(cdf_item, DataModelResponse)
            yield from self._data_model_insights(item_id, resource.source_path, request, cdf_item)

    # A view's base (container-mapped) property may have its name, description, container and
    # containerPropertyIdentifier changed freely without a version bump; remapping a property to a new
    # container/identifier is how consumers are pointed at new data without forcing a version change.
    _VIEW_PROPERTY_METADATA_FIELDS: ClassVar[frozenset[str]] = frozenset({"name", "description"})
    _VIEW_BASE_PROPERTY_ALLOWED_FIELDS: ClassVar[frozenset[str]] = _VIEW_PROPERTY_METADATA_FIELDS | frozenset(
        {"container", "container_property_identifier"}
    )

    @staticmethod
    def _is_disallowed_view_property_change(
        local_property: ViewRequestProperty, cdf_property: ViewRequestProperty
    ) -> bool:
        """Whether changing a view property from its currently deployed definition to the local definition
        is an operation CDF will not apply without bumping the view version.

        A base property mapped to a container may have its metadata and container mapping changed freely.
        Connection properties (edge and reverse direct relation) only allow changing name and description;
        any other field (source, type, direction, through, or switching single/multi) is a breaking change
        per CDF's view update rules.
        """
        if type(local_property) is not type(cdf_property):
            return True
        allowed_fields = (
            DependencyRuleSet._VIEW_BASE_PROPERTY_ALLOWED_FIELDS
            if isinstance(local_property, ViewCorePropertyRequest)
            else DependencyRuleSet._VIEW_PROPERTY_METADATA_FIELDS
        )
        changed_fields = {
            field_name
            for field_name in type(local_property).model_fields
            if field_name != "connection_type"
            and getattr(local_property, field_name) != getattr(cdf_property, field_name)
        }
        return not changed_fields.issubset(allowed_fields)

    @staticmethod
    def _is_disallowed_container_property_change(
        local_property: ContainerPropertyDefinition, cdf_property: ContainerPropertyDefinition
    ) -> bool:
        """Whether changing a container property from its currently deployed definition to the local
        definition is an operation CDF will reject.

        Per CDF's container property update rules: the type (including list state, collation and, for
        direct relations, the target container), and autoIncrement can never change. A property can go
        from nullable to non-nullable, but not the other way around. Everything else (name, description,
        defaultValue) is metadata and can always change.

        Fields left unset locally (list, collation, ...) fall back to CDF's own defaults, so an omitted
        field is never treated as a change: only fields the local YAML actually set are compared.
        """
        local_type, cdf_type = local_property.type, cdf_property.type
        if type(local_type) is not type(cdf_type) or any(
            field_name != "type" and getattr(local_type, field_name) != getattr(cdf_type, field_name)
            for field_name in local_type.model_fields_set
        ):
            return True
        if (
            "auto_increment" in local_property.model_fields_set
            and local_property.auto_increment != cdf_property.auto_increment
        ):
            return True
        if cdf_property.nullable is False and local_property.nullable is True:
            return True
        return False

    def _container_insights(
        self,
        container_id: ContainerId,
        source_path: AbsoluteFilePath,
        local_request: ContainerRequest,
        cdf_response: ContainerResponse,
    ) -> Iterable[ConsistencyError]:
        local_properties = local_request.properties
        cdf_properties = cdf_response.properties

        changed = sorted(
            name
            for name in set(cdf_properties) & set(local_properties)
            if self._is_disallowed_container_property_change(local_properties[name], cdf_properties[name])
        )

        if changed:
            removed = sorted(set(cdf_properties) - set(local_properties))
            affected = humanize_collection([f"{name!r}" for name in sorted({*removed, *changed})])
            yield ConsistencyError(
                code=self.INVALID_OPERATION_CODE,
                message=(
                    f"Local config for container {container_id} has some properties {affected} that have been modified in a way CDF "
                    f"does not support. Deploying the current local YAML config will not apply these changes to the container in CDF."
                ),
                fix=(
                    f"Revert the properties to match the deployed version, or use 'cdf modules pull' to sync your local container config with the deployed version. See {URL.dm_changes_docs}."
                ),
                source_files=[source_path],
            )
        missing = sorted(set(cdf_properties) - set(local_properties))
        if missing and not changed:
            yield ConsistencyError(
                code=self.INVALID_OPERATION_CODE,
                message=(
                    f"Local config for container {container_id} is missing properties "
                    f"{humanize_collection([f'{name!r}' for name in missing])} that have previously been deployed to CDF. "
                    f"Deploying the current local YAML config will not remove them from the container in CDF, since this is not a supported operation."
                ),
                fix=(
                    f"Add the properties back to your local YAML config, or use 'cdf modules pull' to sync your local container config with the deployed version. See {URL.dm_changes_docs}."
                ),
                source_files=[source_path],
            )

        # usedFor defaults to "node" when omitted, both locally and by CDF, so an omitted local value is
        # treated as an explicit "node" request rather than "no change requested".
        local_used_for = local_request.used_for or "node"
        if local_used_for != cdf_response.used_for:
            # usedFor cannot change once set; every other top-level container field (name, description)
            # is metadata and CDF applies changes to it without restriction, so it is not checked here.
            yield ConsistencyError(
                code=self.INVALID_OPERATION_CODE,
                message=(
                    f"Local config for container {container_id} has modified usedFor ('{local_used_for}') compared to the deployed "
                    f"container in CDF ('{cdf_response.used_for}'). CDF does not support changing the usedFor of an existing container, so deploying the current "
                    f"local YAML config will not apply this change to the container in CDF."
                ),
                fix=(
                    f"Revert usedFor back to '{cdf_response.used_for}', or use 'cdf modules pull' to sync your local container config with the deployed version. See {URL.dm_changes_docs}."
                ),
                source_files=[source_path],
            )

    def _view_insights(
        self,
        view_id: ViewId,
        source_path: AbsoluteFilePath,
        local_request: ViewRequest,
        cdf_response: ViewResponse,
    ) -> Iterable[ConsistencyError]:
        cdf_as_request = cdf_response.as_request_resource()
        local_properties = local_request.properties or {}
        cdf_properties = cdf_as_request.properties or {}

        removed = sorted(set(cdf_properties) - set(local_properties))
        changed = sorted(
            name
            for name in set(cdf_properties) & set(local_properties)
            if self._is_disallowed_view_property_change(local_properties[name], cdf_properties[name])
        )
        if changed:
            affected = humanize_collection([f"{name!r}" for name in sorted({*removed, *changed})])
            yield ConsistencyError(
                code=self.INVALID_OPERATION_CODE,
                message=(
                    f"Local config for view {view_id} has some properties {affected} that have been modified in a way CDF "
                    f"does not support without a version bump. Deploying the current local YAML config will not apply "
                    f"these changes to the view in CDF unless you update the view version."
                ),
                fix=(
                    f"Update the view version to apply the change, or use 'cdf modules pull' to sync your local view config with the deployed version. See {URL.dm_changes_docs}."
                ),
                source_files=[source_path],
            )
        elif removed:
            yield ConsistencyError(
                code=self.INVALID_OPERATION_CODE,
                message=(
                    f"Local config for view {view_id} is missing properties "
                    f"{humanize_collection([f'{name!r}' for name in removed])} that have previously been deployed to CDF for the "
                    f"same view version. Deploying the current local YAML config will not remove them from the view in CDF unless you update the view version."
                ),
                fix=(
                    f"Update the view version to apply the change, or use 'cdf modules pull' to sync your local view config with the deployed version. See {URL.dm_changes_docs}."
                ),
                source_files=[source_path],
            )
        if (local_request.implements or []) != (cdf_as_request.implements or []):
            # implements can break clients relying on inherited properties, so it requires a version bump.
            # name, description and filter are metadata/query-only and can always change.
            yield ConsistencyError(
                code=self.INVALID_OPERATION_CODE,
                message=(
                    f"Local config for view {view_id} has changed implements compared to the view version already deployed to CDF"
                ),
                fix=(
                    f"Update the view version to apply the change, or use 'cdf modules pull' to sync your local view config with the deployed version. See {URL.dm_changes_docs}."
                ),
                source_files=[source_path],
            )

    def _data_model_insights(
        self,
        data_model_id: DataModelId,
        source_path: AbsoluteFilePath,
        local_request: DataModelRequest,
        cdf_response: DataModelResponse,
    ) -> Iterable[ConsistencyError]:
        local_views = set(local_request.views or [])
        cdf_views = set(cdf_response.views or [])
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
                    f"Local config for data model {data_model_id} has changed the view version of {changes} compared to the existing deployed data model version in CDF. "
                    "View version used by a data model can only be updated if you also update the data model version."
                ),
                fix=(
                    f"Update the data model version to apply the change, or use 'cdf modules pull' to sync your local data model config with the deployed version. See {URL.dm_changes_docs}."
                ),
                source_files=[source_path],
            )

        if removed:
            yield ConsistencyError(
                code=self.INVALID_OPERATION_CODE,
                message=(
                    f"Local config for data model {data_model_id} is missing the view(s) "
                    f"{humanize_collection([f'{view_id!s}' for view_id in removed])} compared to the existing deployed data model version in CDF. "
                    f"Deploying the current local YAML config will not remove them from the data model in CDF unless you update the data model version."
                ),
                fix=(
                    f"Update the data model version to apply the change, or use 'cdf modules pull' to sync your local data model config with the deployed version. See {URL.dm_changes_docs}."
                ),
                source_files=[source_path],
            )
