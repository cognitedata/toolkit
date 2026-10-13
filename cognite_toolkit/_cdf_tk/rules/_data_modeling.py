from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, ClassVar, Literal, TypeVar, cast

from cognite_toolkit._cdf_tk.client import ToolkitClient, _resource_base
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import ContainerDirectId, ContainerId, DataModelId, ViewDirectId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    ContainerPropertyDefinition,
    ContainerRequest,
    ContainerResponse,
    DataModelRequest,
    DataModelResponse,
    DirectNodeRelation,
    ReverseDirectRelationProperty,
    ViewCorePropertyRequest,
    ViewCorePropertyResponse,
    ViewRequest,
    ViewRequestProperty,
    ViewResponse,
    ViewResponseProperty,
)
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuiltResource, contains_unresolved_variable
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import (
    BuildError,
    BuildWarning,
    Insight,
)
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._types import AbsoluteFilePath
from cognite_toolkit._cdf_tk.constants import URL
from cognite_toolkit._cdf_tk.resource_ios import ContainerIO, DataModelIO, ResourceIO, ViewIO
from cognite_toolkit._cdf_tk.utils import humanize_collection

from ._base import (
    InternalValidatorException,
    RuleSetStatus,
    ToolkitGlobalRuleSet,
    quote_identifier,
    with_position,
)

_KeyT = TypeVar("_KeyT")
_ReferenceStatus = Literal["ok", "not_direct", "missing"]
_LocalContainer = tuple[BuiltResource, ContainerRequest]
_LocalView = tuple[BuiltResource, ViewRequest]


@dataclass(frozen=True)
class _ContainerPropertyReference:
    """A view property mapped onto a container property."""

    container_id: ContainerId
    property_identifier: str
    resources: tuple[BuiltResource, ...]

    @property
    def label(self) -> str:
        return f"{self.container_id}.{self.property_identifier}"


@dataclass(frozen=True)
class _ReverseDirectRelationReference:
    """A reverse direct relation pointing at a property through a view or container."""

    through: ContainerDirectId | ViewDirectId
    resources: tuple[BuiltResource, ...]


@dataclass(frozen=True)
class _SchemaReferences:
    container_properties: tuple[_ContainerPropertyReference, ...]
    reverse_direct_relations: tuple[_ReverseDirectRelationReference, ...]


class DataModelingRuleSet(ToolkitGlobalRuleSet):
    """Validates view-to-container mappings, reverse direct relations, and data modeling state changes.

    Container properties referenced by views, and the direct relations behind reverse relations, are
    resolved against local modules first. Anything still missing is checked in CDF when a client is
    available.
    """

    CODE_PREFIX = "DATA-MODELING"
    DISPLAY_NAME = "Data modeling"

    def get_status(self) -> RuleSetStatus:
        if self.client is None:
            return RuleSetStatus(
                code="reduced",
                message=(
                    "No client provided, will only validate data modeling references between resources "
                    "within the provided modules, but not validate against CDF."
                ),
            )
        return RuleSetStatus(
            code="ready",
            message="Will validate data modeling references and state changes against CDF.",
        )

    def validate(self) -> Iterable[Insight | InternalValidatorException]:
        yield from self._check_schema_references()
        if self.client is not None:
            yield from self._validate_data_modeling_changes(self.client)

    def _check_schema_references(self) -> Iterable[Insight | InternalValidatorException]:
        try:
            local_containers = self._load_containers()
            local_views = self._load_views()
        except Exception as e:
            yield InternalValidatorException(
                message=f"Failed to load local containers and views: {e}",
                source="DataModeling",
            )
            return
        references = self._find_references(local_views)
        yield from self._validate_references(references, local_containers, local_views)

    @classmethod
    def _find_references(cls, local_views: dict[ViewId, _LocalView]) -> _SchemaReferences:
        container_properties: dict[tuple[ContainerId, str], list[BuiltResource]] = defaultdict(list)
        reverse_relations: dict[ContainerDirectId | ViewDirectId, list[BuiltResource]] = defaultdict(list)
        for resource, view in local_views.values():
            for prop in (view.properties or {}).values():
                if isinstance(prop, ViewCorePropertyRequest):
                    cls._append(container_properties, (prop.container, prop.container_property_identifier), resource)
                elif isinstance(prop, ReverseDirectRelationProperty):
                    cls._append(reverse_relations, prop.through, resource)
        return _SchemaReferences(
            container_properties=tuple(
                _ContainerPropertyReference(container_id, property_identifier, tuple(resources))
                for (container_id, property_identifier), resources in container_properties.items()
            ),
            reverse_direct_relations=tuple(
                _ReverseDirectRelationReference(through, tuple(resources))
                for through, resources in reverse_relations.items()
            ),
        )

    def _validate_references(
        self,
        references: _SchemaReferences,
        local_containers: dict[ContainerId, _LocalContainer],
        local_views: dict[ViewId, _LocalView],
    ) -> Iterable[Insight | InternalValidatorException]:
        missing_container_properties: list[_ContainerPropertyReference] = []
        for property_ref in references.container_properties:
            if contains_unresolved_variable(property_ref.label):
                continue
            if (
                self._container_property(
                    property_ref.container_id, property_ref.property_identifier, local_containers, {}
                )
                is None
            ):
                missing_container_properties.append(property_ref)

        invalid_reverses: list[_ReverseDirectRelationReference] = []
        missing_reverses: list[_ReverseDirectRelationReference] = []
        for relation_ref in references.reverse_direct_relations:
            if contains_unresolved_variable(relation_ref.through):
                continue
            status = self._reverse_status(relation_ref.through, local_views, local_containers, {}, {})
            if status == "not_direct":
                invalid_reverses.append(relation_ref)
            elif status == "missing":
                missing_reverses.append(relation_ref)

        for relation_ref in invalid_reverses:
            yield from self._not_direct_errors(relation_ref)
        if not missing_container_properties and not missing_reverses:
            return
        if self.client is None:
            yield from self._unverified_properties(missing_container_properties)
            yield from self._unverified_reverses(missing_reverses)
            return
        yield from self._validate_missing_in_cdf(
            missing_container_properties, missing_reverses, local_containers, local_views
        )

    def _validate_missing_in_cdf(
        self,
        missing_properties: list[_ContainerPropertyReference],
        missing_reverses: list[_ReverseDirectRelationReference],
        local_containers: dict[ContainerId, _LocalContainer],
        local_views: dict[ViewId, _LocalView],
    ) -> Iterable[Insight | InternalValidatorException]:
        container_ids, view_ids = self._ids_to_fetch(missing_properties, missing_reverses, local_views)
        cdf_containers, container_error = self._retrieve_containers(container_ids)
        if container_error is not None:
            yield container_error
        cdf_views, view_error = self._retrieve_views(view_ids)
        if view_error is not None:
            yield view_error

        if container_error is None:
            for property_ref in missing_properties:
                if (
                    self._container_property(
                        property_ref.container_id, property_ref.property_identifier, local_containers, cdf_containers
                    )
                    is None
                ):
                    yield from self._unknown_property_errors(property_ref)

        for relation_ref in missing_reverses:
            needs_container, needs_view = self._reverse_remote_need(relation_ref.through, local_views)
            if (needs_container and container_error is not None) or (needs_view and view_error is not None):
                continue
            status = self._reverse_status(
                relation_ref.through, local_views, local_containers, cdf_views, cdf_containers
            )
            if status == "ok":
                continue
            if status == "not_direct":
                yield from self._not_direct_errors(relation_ref)
                continue
            yield from self._unknown_reverse_errors(relation_ref)

    def _ids_to_fetch(
        self,
        missing_properties: list[_ContainerPropertyReference],
        missing_reverses: list[_ReverseDirectRelationReference],
        local_views: dict[ViewId, _LocalView],
    ) -> tuple[set[ContainerId], set[ViewId]]:
        container_ids = {ref.container_id for ref in missing_properties}
        view_ids: set[ViewId] = set()
        for ref in missing_reverses:
            needs_container, needs_view = self._reverse_remote_need(ref.through, local_views)
            if isinstance(ref.through, ContainerDirectId) and needs_container:
                container_ids.add(ref.through.source)
                continue
            if not isinstance(ref.through, ViewDirectId):
                continue
            if needs_view:
                view_ids.add(ref.through.source)
                continue
            local = local_views.get(ref.through.source)
            prop = (local[1].properties or {}).get(ref.through.identifier) if local is not None else None
            if isinstance(prop, ViewCorePropertyRequest):
                container_ids.add(prop.container)
        return container_ids, view_ids

    @staticmethod
    def _reverse_remote_need(
        through: ContainerDirectId | ViewDirectId,
        local_views: dict[ViewId, _LocalView],
    ) -> tuple[bool, bool]:
        """Whether a still-missing reverse relation must be resolved via a container or a view lookup."""
        if isinstance(through, ContainerDirectId):
            return True, False
        local = local_views.get(through.source)
        prop = (local[1].properties or {}).get(through.identifier) if local is not None else None
        if isinstance(prop, ViewCorePropertyRequest):
            return True, False
        return False, True

    def _reverse_status(
        self,
        through: ContainerDirectId | ViewDirectId,
        local_views: dict[ViewId, _LocalView],
        local_containers: dict[ContainerId, _LocalContainer],
        cdf_views: dict[ViewId, ViewResponse],
        cdf_containers: dict[ContainerId, ContainerResponse],
    ) -> _ReferenceStatus:
        if isinstance(through, ContainerDirectId):
            return self._container_property_status(through.source, through.identifier, local_containers, cdf_containers)
        prop = self._find_view_property(through.source, through.identifier, local_views, cdf_views)
        if prop is None:
            return "missing"
        return self._view_property_direct_status(prop, local_containers, cdf_containers)

    @classmethod
    def _view_property_direct_status(
        cls,
        prop: ViewRequestProperty | ViewResponseProperty,
        local_containers: dict[ContainerId, _LocalContainer],
        cdf_containers: dict[ContainerId, ContainerResponse],
    ) -> _ReferenceStatus:
        if isinstance(prop, ViewCorePropertyResponse):
            if isinstance(prop.type, DirectNodeRelation):
                return "ok"
            return "not_direct"
        if isinstance(prop, ViewCorePropertyRequest):
            return cls._container_property_status(
                prop.container, prop.container_property_identifier, local_containers, cdf_containers
            )
        return "not_direct"

    @classmethod
    def _container_property_status(
        cls,
        container_id: ContainerId,
        property_identifier: str,
        local_containers: dict[ContainerId, _LocalContainer],
        cdf_containers: dict[ContainerId, ContainerResponse],
    ) -> _ReferenceStatus:
        prop = cls._container_property(container_id, property_identifier, local_containers, cdf_containers)
        if prop is None:
            return "missing"
        if isinstance(prop.type, DirectNodeRelation):
            return "ok"
        return "not_direct"

    @staticmethod
    def _find_view_property(
        view_id: ViewId,
        property_identifier: str,
        local_views: dict[ViewId, _LocalView],
        cdf_views: dict[ViewId, ViewResponse],
    ) -> ViewRequestProperty | ViewResponseProperty | None:
        local = local_views.get(view_id)
        if local is not None:
            prop = (local[1].properties or {}).get(property_identifier)
            if prop is not None:
                return prop
        cdf_view = cdf_views.get(view_id)
        if cdf_view is None:
            return None
        return cdf_view.properties.get(property_identifier)

    @staticmethod
    def _container_property(
        container_id: ContainerId,
        property_identifier: str,
        local_containers: dict[ContainerId, _LocalContainer],
        cdf_containers: dict[ContainerId, ContainerResponse],
    ) -> ContainerPropertyDefinition | None:
        local = local_containers.get(container_id)
        if local is not None and property_identifier in local[1].properties:
            return local[1].properties[property_identifier]
        cdf_container = cdf_containers.get(container_id)
        if cdf_container is not None and property_identifier in cdf_container.properties:
            return cdf_container.properties[property_identifier]
        return None

    def _load_containers(self) -> dict[ContainerId, _LocalContainer]:
        crud = ContainerIO(self._load_client())
        loaded: dict[ContainerId, _LocalContainer] = {}
        for module in self.modules:
            for item_id, (resource, request) in module.load_local_resources(crud).items():
                if isinstance(item_id, ContainerId):
                    loaded[item_id] = (resource, request)
        return loaded

    def _load_views(self) -> dict[ViewId, _LocalView]:
        crud = ViewIO(self._load_client())
        loaded: dict[ViewId, _LocalView] = {}
        for module in self.modules:
            for item_id, (resource, request) in module.load_local_resources(crud).items():
                if isinstance(item_id, ViewId):
                    loaded[item_id] = (resource, request)
        return loaded

    def _load_client(self) -> ToolkitClient:
        """Client used only to read built YAML. File loading does not call CDF."""
        if self.client is not None:
            return self.client
        return cast(ToolkitClient, SimpleNamespace(console=None))

    def _retrieve_containers(
        self, container_ids: set[ContainerId]
    ) -> tuple[dict[ContainerId, ContainerResponse], InternalValidatorException | None]:
        if not container_ids or self.client is None:
            return {}, None
        try:
            items = ContainerIO(self.client).retrieve(list(container_ids))
        except ToolkitAPIError as e:
            return {}, InternalValidatorException(
                message=f"Failed to verify existence of {ContainerIO.kind.lower()} in CDF: {e}",
                source=ContainerIO.kind,
            )
        return {item.as_id(): item for item in items}, None

    def _retrieve_views(
        self, view_ids: set[ViewId]
    ) -> tuple[dict[ViewId, ViewResponse], InternalValidatorException | None]:
        if not view_ids or self.client is None:
            return {}, None
        try:
            items = ViewIO(self.client).retrieve(list(view_ids))
        except ToolkitAPIError as e:
            return {}, InternalValidatorException(
                message=f"Failed to verify existence of {ViewIO.kind.lower()} in CDF: {e}",
                source=ViewIO.kind,
            )
        return {item.as_id(): item for item in items}, None

    def _unverified_properties(self, missing_properties: list[_ContainerPropertyReference]) -> Iterable[Insight]:
        for ref in missing_properties:
            for resource in ref.resources:
                yield with_position(
                    BuildWarning(
                        code="UNVERIFIED-REFERENCED-PROPERTY",
                        message=(
                            f"Missing container property '{ref.label}'. "
                            f"It is referenced by {self._referenced_by(resource)}."
                        ),
                        fix=(
                            "Provide credentials to enable CDF verification, or manually either:"
                            "1) ensure that the container property exists, or 2) remove the reference to it."
                        ),
                        source_file=resource.source_path,
                    ),
                    values=[ref.property_identifier],
                    variables=resource.variables,
                )

    def _unverified_reverses(self, missing_reverses: list[_ReverseDirectRelationReference]) -> Iterable[Insight]:
        for ref in missing_reverses:
            for resource in ref.resources:
                yield with_position(
                    BuildWarning(
                        code="UNVERIFIED-REFERENCED-PROPERTY",
                        message=(
                            f"Missing direct relation '{ref.through}'. "
                            f"It is referenced by {self._referenced_by(resource)}."
                        ),
                        fix=(
                            "Provide credentials to enable CDF verification, or manually either:"
                            "1) ensure that the container property exists, or 2) remove the reference to it."
                        ),
                        source_file=resource.source_path,
                    ),
                    values=[ref.through.identifier],
                    variables=resource.variables,
                )

    def _unknown_property_errors(self, ref: _ContainerPropertyReference) -> Iterable[Insight]:
        for resource in ref.resources:
            yield with_position(
                BuildError(
                    code="MISSING-REFERENCED-PROPERTY",
                    message=f"Container property '{ref.label}' does not exist locally or in CDF. "
                    f"It is referenced by {quote_identifier(resource.identifier)}.",
                    fix="Ensure that the container property exists or remove the reference to it.",
                    source_file=resource.source_path,
                ),
                values=[ref.property_identifier],
                variables=resource.variables,
            )

    def _unknown_reverse_errors(self, ref: _ReverseDirectRelationReference) -> Iterable[Insight]:
        for resource in ref.resources:
            yield with_position(
                BuildError(
                    code="MISSING-REFERENCED-PROPERTY",
                    message=f"Direct relation '{ref.through}' does not exist locally or in CDF. "
                    f"It is referenced by {quote_identifier(resource.identifier)}.",
                    fix="Ensure that the direct relation exists or remove the reference to it.",
                    source_file=resource.source_path,
                ),
                values=[ref.through.identifier],
                variables=resource.variables,
            )

    def _not_direct_errors(self, ref: _ReverseDirectRelationReference) -> Iterable[Insight]:
        for resource in ref.resources:
            yield with_position(
                BuildError(
                    code="INVALID-REFERENCED-PROPERTY",
                    message=(
                        f"Reverse direct relation through '{ref.through}' points at '{ref.through.identifier}', "
                        "which is not a direct relation."
                    ),
                    fix="Point the reverse direct relation at a property of type direct.",
                    source_file=resource.source_path,
                ),
                values=[ref.through.identifier],
                variables=resource.variables,
            )

    @staticmethod
    def _referenced_by(resource: BuiltResource) -> str:
        return quote_identifier(resource.identifier)

    @staticmethod
    def _append(bucket: dict[_KeyT, list[BuiltResource]], key: _KeyT, resource: BuiltResource) -> None:
        resources = bucket[key]
        if resource not in resources:
            resources.append(resource)

    def _validate_data_modeling_changes(self, client: ToolkitClient) -> Iterable[Insight | InternalValidatorException]:
        """Reports local container, view and data model changes that CDF will silently drop on deploy.

        Containers cannot have properties removed, and views and data models are immutable per version.
        Today this is only discovered during (or after) ``cdf deploy``. This surfaces the same findings
        during ``cdf build``.
        """
        yield from self._compare_data_modeling_resource(ContainerIO(client))
        yield from self._compare_data_modeling_resource(ViewIO(client))
        yield from self._compare_data_modeling_resource(DataModelIO(client))

    def _compare_data_modeling_resource(
        self,
        crud: ResourceIO[
            _resource_base.T_Identifier, _resource_base.T_RequestResource, _resource_base.T_ResponseResource, Any
        ],
    ) -> Iterable[Insight | InternalValidatorException]:
        """Compare one data-modeling resource type against CDF."""
        try:
            local_by_id: dict[_resource_base.T_Identifier, tuple[BuiltResource, _resource_base.T_RequestResource]] = {}
            for module in self.modules:
                local_by_id.update(module.load_local_resources(crud))
            if not local_by_id:
                return
            cdf_items = crud.retrieve(list(local_by_id.keys()))
        except ToolkitAPIError as e:
            yield InternalValidatorException(
                message=f"Failed to compare local {crud.display_name} with CDF: {e}",
                source=crud.kind,
            )
            return
        for cdf_item in cdf_items:
            item_id = crud.get_id(cdf_item)
            if item_id not in local_by_id:
                continue
            resource, request = local_by_id[item_id]
            yield from self._as_data_modeling_insights(item_id, resource, request, cdf_item)

    def _as_data_modeling_insights(
        self,
        item_id: _resource_base.Identifier,
        resource: BuiltResource,
        request: Any,
        cdf_item: Any,
    ) -> Iterable[Insight]:
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

    @classmethod
    def _is_disallowed_view_property_change(
        cls, local_property: ViewRequestProperty, cdf_property: ViewRequestProperty
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
            cls._VIEW_BASE_PROPERTY_ALLOWED_FIELDS
            if isinstance(local_property, ViewCorePropertyRequest)
            else cls._VIEW_PROPERTY_METADATA_FIELDS
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
    ) -> Iterable[Insight]:
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
            yield with_position(
                BuildError(
                    code="INVALID-RESOURCE-CHANGE",
                    message=(
                        f"Local config for container {container_id} has some properties {affected} that have been modified in a way CDF "
                        f"does not support. Deploying the current local YAML config will not apply these changes to the container in CDF."
                    ),
                    fix=(
                        f"Revert the properties to match the deployed version, or use 'cdf modules pull' to sync your local container config with the deployed version. See {URL.dm_changes_docs}."
                    ),
                    source_file=source_path,
                ),
                keys=[*changed, "properties"],
            )
        missing = sorted(set(cdf_properties) - set(local_properties))
        if missing and not changed:
            yield with_position(
                BuildWarning(
                    code="UNSUPPORTED-RESOURCE-REMOVAL",
                    message=(
                        f"Local config for container {container_id} is missing properties "
                        f"{humanize_collection([f'{name!r}' for name in missing])} that have previously been deployed to CDF. "
                        f"Deploying the current local YAML config will not remove them from the container in CDF, since this is not a supported operation."
                    ),
                    fix=(
                        f"Add the properties back to your local YAML config, or use 'cdf modules pull' to sync your local container config with the deployed version. See {URL.dm_changes_docs}."
                    ),
                    source_file=source_path,
                ),
                keys=["properties"],
            )

        # usedFor defaults to "node" when omitted, both locally and by CDF, so an omitted local value is
        # treated as an explicit "node" request rather than "no change requested".
        local_used_for = local_request.used_for or "node"
        if local_used_for != cdf_response.used_for:
            # usedFor cannot change once set; every other top-level container field (name, description)
            # is metadata and CDF applies changes to it without restriction, so it is not checked here.
            yield with_position(
                BuildError(
                    code="INVALID-RESOURCE-CHANGE",
                    message=(
                        f"Local config for container {container_id} has modified usedFor ('{local_used_for}') compared to the deployed "
                        f"container in CDF ('{cdf_response.used_for}'). CDF does not support changing the usedFor of an existing container, so deploying the current "
                        f"local YAML config will not apply this change to the container in CDF."
                    ),
                    fix=(
                        f"Revert usedFor back to '{cdf_response.used_for}', or use 'cdf modules pull' to sync your local container config with the deployed version. See {URL.dm_changes_docs}."
                    ),
                    source_file=source_path,
                ),
                keys=["usedFor"],
            )

    def _view_insights(
        self,
        view_id: ViewId,
        source_path: AbsoluteFilePath,
        local_request: ViewRequest,
        cdf_response: ViewResponse,
    ) -> Iterable[Insight]:
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
            yield with_position(
                BuildError(
                    code="INVALID-RESOURCE-CHANGE",
                    message=(
                        f"Local config for view {view_id} has some properties {affected} that have been modified in a way CDF "
                        f"does not support without a version bump. Deploying the current local YAML config will not apply "
                        f"these changes to the view in CDF unless you update the view version."
                    ),
                    fix=(
                        f"Update the view version to apply the change, or use 'cdf modules pull' to sync your local view config with the deployed version. See {URL.dm_changes_docs}."
                    ),
                    source_file=source_path,
                ),
                keys=[*changed, "properties"],
            )
        elif removed:
            yield with_position(
                BuildWarning(
                    code="UNSUPPORTED-RESOURCE-REMOVAL",
                    message=(
                        f"Local config for view {view_id} is missing properties "
                        f"{humanize_collection([f'{name!r}' for name in removed])} that have previously been deployed to CDF for the "
                        f"same view version. Deploying the current local YAML config will not remove them from the view in CDF unless you update the view version."
                    ),
                    fix=(
                        f"Update the view version to apply the change, or use 'cdf modules pull' to sync your local view config with the deployed version. See {URL.dm_changes_docs}."
                    ),
                    source_file=source_path,
                ),
                keys=["properties"],
            )
        if (local_request.implements or []) != (cdf_as_request.implements or []):
            # implements can break clients relying on inherited properties, so it requires a version bump.
            # name, description and filter are metadata/query-only and can always change.
            yield with_position(
                BuildError(
                    code="INVALID-RESOURCE-CHANGE",
                    message=(
                        f"Local config for view {view_id} has changed implements compared to the view version already deployed to CDF"
                    ),
                    fix=(
                        f"Update the view version to apply the change, or use 'cdf modules pull' to sync your local view config with the deployed version. See {URL.dm_changes_docs}."
                    ),
                    source_file=source_path,
                ),
                keys=["implements"],
            )

    def _data_model_insights(
        self,
        data_model_id: DataModelId,
        source_path: AbsoluteFilePath,
        local_request: DataModelRequest,
        cdf_response: DataModelResponse,
    ) -> Iterable[Insight]:
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
            insight = BuildError(
                code="INVALID-RESOURCE-CHANGE",
                message=(
                    f"Local config for data model {data_model_id} has changed the view version of {changes} compared to the existing deployed data model version in CDF. "
                    "View version used by a data model can only be updated if you also update the data model version."
                ),
                fix=(
                    f"Update the data model version to apply the change, or use 'cdf modules pull' to sync your local data model config with the deployed version. See {URL.dm_changes_docs}."
                ),
                source_file=source_path,
            )
            insight = with_position(insight, values=[view_id.external_id for view_id, _ in version_changed])
            if insight.line is None:
                insight = with_position(insight, keys=["views"])
            yield insight

        if removed:
            yield with_position(
                BuildWarning(
                    code="UNSUPPORTED-RESOURCE-REMOVAL",
                    message=(
                        f"Local config for data model {data_model_id} is missing the view(s) "
                        f"{humanize_collection([f'{view_id!s}' for view_id in removed])} compared to the existing deployed data model version in CDF. "
                        f"Deploying the current local YAML config will not remove them from the data model in CDF unless you update the data model version."
                    ),
                    fix=(
                        f"Update the data model version to apply the change, or use 'cdf modules pull' to sync your local data model config with the deployed version. See {URL.dm_changes_docs}."
                    ),
                    source_file=source_path,
                ),
                keys=["views"],
            )
