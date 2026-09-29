from collections import defaultdict
from collections.abc import Iterable
from types import SimpleNamespace
from typing import Literal, TypeVar, cast

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import ContainerDirectId, ContainerId, ViewDirectId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    ContainerPropertyDefinition,
    ContainerRequest,
    ContainerResponse,
    DirectNodeRelation,
    ReverseDirectRelationProperty,
    ViewCorePropertyRequest,
    ViewCorePropertyResponse,
    ViewRequest,
    ViewRequestProperty,
    ViewResponse,
    ViewResponseProperty,
)
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuiltResource
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import ConsistencyError, Insight
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._types import AbsoluteFilePath
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.resource_ios import ContainerCRUD, ViewIO
from cognite_toolkit._cdf_tk.utils.file import relative_to_if_possible

from ._base import InternalValidatorException, RuleSetStatus, ToolkitGlobalRuleSet
from ._dependencies import DependencyRuleSet

_KeyT = TypeVar("_KeyT")
_ReferenceStatus = Literal["ok", "not_direct", "missing"]
_LocalContainer = tuple[BuiltResource, ContainerRequest]
_LocalView = tuple[BuiltResource, ViewRequest]


class DataModelingRuleSet(ToolkitGlobalRuleSet):
    """Validates view-to-container mappings, reverse direct relations, and data modeling state changes.

    Container properties referenced by views, and the direct relations behind reverse relations, are
    resolved against local modules first. Anything still missing is checked in CDF when a client is
    available.
    """

    CODE_PREFIX = "DATA-MODELING"
    DISPLAY_NAME = "Data modeling checks"
    UNKNOWN_REFERENCE = "UNKNOWN-REFERENCE"
    UNVERIFIED_REFERENCE = "UNVERIFIED-REFERENCE"
    INVALID_REFERENCE = "INVALID-REFERENCE"

    def get_status(self) -> RuleSetStatus:
        if not Flags.ALPHA_RULES.is_enabled():
            return RuleSetStatus(
                code="skip",
                message="Alpha rules are disabled. Enable the alpha rules flag to run data modeling checks.",
            )
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
        if not Flags.ALPHA_RULES.is_enabled():
            return
        yield from self._validate_references()
        if self.client is not None:
            yield from self._validate_data_modeling_changes(self.client)

    def _validate_data_modeling_changes(
        self, client: ToolkitClient
    ) -> Iterable[ConsistencyError | InternalValidatorException]:
        """Reports local container, view and data model changes that CDF will silently drop on deploy.

        DependencyRuleSet runs the same checks when alpha rules are disabled.
        """
        yield from DependencyRuleSet(self.modules, client)._validate_data_modeling_changes(client)

    def _validate_references(self) -> Iterable[ConsistencyError | InternalValidatorException]:
        try:
            local_containers = self._load_containers()
            local_views = self._load_views()
        except Exception as e:
            yield InternalValidatorException(
                message=f"Failed to load local containers and views: {e}",
                source="DataModeling",
            )
            return

        container_refs: dict[tuple[ContainerId, str], list[BuiltResource]] = defaultdict(list)
        reverse_refs: dict[ContainerDirectId | ViewDirectId, list[BuiltResource]] = defaultdict(list)
        for resource, view in local_views.values():
            self._collect_view_dependencies(resource, view, container_refs, reverse_refs)

        missing_properties: dict[tuple[ContainerId, str], list[BuiltResource]] = {}
        for key, resources in container_refs.items():
            container_id, property_identifier = key
            if self._container_property(container_id, property_identifier, local_containers, {}) is not None:
                continue
            missing_properties[key] = resources

        missing_reverses: dict[ContainerDirectId | ViewDirectId, list[BuiltResource]] = {}
        for through, resources in reverse_refs.items():
            status = self._reverse_status(through, local_views, local_containers, {}, {})
            if status == "ok":
                continue
            if status == "not_direct":
                yield self._not_direct_error(through, resources)
                continue
            missing_reverses[through] = resources

        if not missing_properties and not missing_reverses:
            return

        if self.client is None:
            yield from self._unverified_properties(missing_properties)
            yield from self._unverified_reverses(missing_reverses)
            return

        container_ids, view_ids = self._ids_to_fetch(missing_properties, missing_reverses, local_views)
        cdf_containers, container_error = self._retrieve_containers(container_ids)
        if container_error is not None:
            yield container_error
        cdf_views, view_error = self._retrieve_views(view_ids)
        if view_error is not None:
            yield view_error

        if container_error is None:
            for (container_id, property_identifier), resources in missing_properties.items():
                if (
                    self._container_property(container_id, property_identifier, local_containers, cdf_containers)
                    is not None
                ):
                    continue
                yield self._unknown_property_error(container_id, property_identifier, resources)

        for through, resources in missing_reverses.items():
            needs_container, needs_view = self._reverse_remote_need(through, local_views)
            if (needs_container and container_error is not None) or (needs_view and view_error is not None):
                continue
            status = self._reverse_status(through, local_views, local_containers, cdf_views, cdf_containers)
            if status == "ok":
                continue
            if status == "not_direct":
                yield self._not_direct_error(through, resources)
                continue
            yield self._unknown_reverse_error(through, resources)

    @classmethod
    def _collect_view_dependencies(
        cls,
        resource: BuiltResource,
        view: ViewRequest,
        container_refs: dict[tuple[ContainerId, str], list[BuiltResource]],
        reverse_refs: dict[ContainerDirectId | ViewDirectId, list[BuiltResource]],
    ) -> None:
        for prop in (view.properties or {}).values():
            if isinstance(prop, ViewCorePropertyRequest):
                cls._append(container_refs, (prop.container, prop.container_property_identifier), resource)
            elif isinstance(prop, ReverseDirectRelationProperty):
                cls._append(reverse_refs, prop.through, resource)

    def _ids_to_fetch(
        self,
        missing_properties: dict[tuple[ContainerId, str], list[BuiltResource]],
        missing_reverses: dict[ContainerDirectId | ViewDirectId, list[BuiltResource]],
        local_views: dict[ViewId, _LocalView],
    ) -> tuple[set[ContainerId], set[ViewId]]:
        container_ids = {container_id for container_id, _ in missing_properties}
        view_ids: set[ViewId] = set()
        for through in missing_reverses:
            needs_container, needs_view = self._reverse_remote_need(through, local_views)
            if isinstance(through, ContainerDirectId) and needs_container:
                container_ids.add(through.source)
                continue
            if not isinstance(through, ViewDirectId):
                continue
            if needs_view:
                view_ids.add(through.source)
                continue
            local = local_views.get(through.source)
            prop = (local[1].properties or {}).get(through.identifier) if local is not None else None
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
        crud = ContainerCRUD(self._load_client())
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
            items = ContainerCRUD(self.client).retrieve(list(container_ids))
        except ToolkitAPIError as e:
            return {}, InternalValidatorException(
                message=f"Failed to verify existence of {ContainerCRUD.kind.lower()} in CDF: {e}",
                source=ContainerCRUD.kind,
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

    def _unverified_properties(
        self, missing_properties: dict[tuple[ContainerId, str], list[BuiltResource]]
    ) -> Iterable[ConsistencyError]:
        for (container_id, property_identifier), resources in missing_properties.items():
            reference = f"{container_id}.{property_identifier}"
            yield ConsistencyError(
                code=self.UNVERIFIED_REFERENCE,
                message=(
                    f"Missing container property '{reference}'. It is referenced by {self._reference_string(resources)}."
                ),
                fix=(
                    "Provide credentials to enable CDF verification. "
                    "Or ensure that the container property exists or remove the reference to it."
                ),
                source_files=self._source_files(resources),
            )

    def _unverified_reverses(
        self, missing_reverses: dict[ContainerDirectId | ViewDirectId, list[BuiltResource]]
    ) -> Iterable[ConsistencyError]:
        for through, resources in missing_reverses.items():
            yield ConsistencyError(
                code=self.UNVERIFIED_REFERENCE,
                message=(
                    f"Missing direct relation '{through}'. It is referenced by {self._reference_string(resources)}."
                ),
                fix=(
                    "Provide credentials to enable CDF verification. "
                    "Or ensure that the direct relation exists or remove the reference to it."
                ),
                source_files=self._source_files(resources),
            )

    def _unknown_property_error(
        self,
        container_id: ContainerId,
        property_identifier: str,
        resources: list[BuiltResource],
    ) -> ConsistencyError:
        return ConsistencyError(
            code=self.UNKNOWN_REFERENCE,
            message=f"Unknown reference to container property '{container_id}.{property_identifier}'",
            fix="Ensure that the container property exists or remove the reference to it.",
            source_files=self._source_files(resources),
        )

    def _unknown_reverse_error(
        self,
        through: ContainerDirectId | ViewDirectId,
        resources: list[BuiltResource],
    ) -> ConsistencyError:
        return ConsistencyError(
            code=self.UNKNOWN_REFERENCE,
            message=f"Unknown reference to direct relation '{through}'",
            fix="Ensure that the direct relation exists or remove the reference to it.",
            source_files=self._source_files(resources),
        )

    def _not_direct_error(
        self,
        through: ContainerDirectId | ViewDirectId,
        resources: list[BuiltResource],
    ) -> ConsistencyError:
        return ConsistencyError(
            code=self.INVALID_REFERENCE,
            message=(
                f"Reverse direct relation through '{through}' points at '{through.identifier}', "
                "which is not a direct relation."
            ),
            fix="Point the reverse direct relation at a property of type direct.",
            source_files=self._source_files(resources),
        )

    @staticmethod
    def _append(bucket: dict[_KeyT, list[BuiltResource]], key: _KeyT, resource: BuiltResource) -> None:
        resources = bucket[key]
        if resource not in resources:
            resources.append(resource)

    @staticmethod
    def _source_files(resources: list[BuiltResource]) -> list[AbsoluteFilePath]:
        return list(dict.fromkeys(resource.source_path for resource in resources))

    @staticmethod
    def _reference_string(resources: list[BuiltResource]) -> str:
        return " - ".join(
            f"{resource.identifier!s} in {relative_to_if_possible(resource.source_path).as_posix()!r}"
            for resource in resources
        )
