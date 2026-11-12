from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar, Literal, Self, TypeVar, cast

from pydantic import BaseModel, ValidationError

from cognite_toolkit._cdf_tk.client import ToolkitClient, _resource_base
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import ContainerDirectId, ContainerId, DataModelId, ViewDirectId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    BtreeIndex,
    ContainerPropertyDefinition,
    ContainerRequest,
    ContainerResponse,
    DataModelRequest,
    DataModelResponse,
    DirectNodeRelation,
    RequiresConstraintDefinition,
    ReverseDirectRelationProperty,
    ViewCorePropertyRequest,
    ViewCorePropertyResponse,
    ViewRequest,
    ViewRequestProperty,
    ViewResponse,
    ViewResponseProperty,
)
from cognite_toolkit._cdf_tk.client.resource_classes.limits import LimitValueResponse
from cognite_toolkit._cdf_tk.client.resource_classes.statistics import ProjectStatisticsResponse
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import contains_unresolved_variable
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import (
    BuildError,
    BuildWarning,
    Insight,
    Recommendation,
)
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._types import AbsoluteFilePath
from cognite_toolkit._cdf_tk.commands.deploy_v2.command import DeploymentResult
from cognite_toolkit._cdf_tk.constants import URL
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.resource_ios import ContainerIO, DataModelIO, ResourceIO, SpaceIO, ViewIO
from cognite_toolkit._cdf_tk.utils import humanize_collection

from ._base import (
    InternalValidatorException,
    ResourceRef,
    RuleSetStatus,
    ToolkitRuleSet,
    identifier_values,
    quote_identifier,
    with_position,
)

_KeyT = TypeVar("_KeyT")
_ReferenceStatus = Literal["ok", "not_direct", "missing"]
_DATA_MODELING_LIMIT_PREFIX = "data_modeling."
_INSTANCES_ENDPOINT = "/models/instances"
_INSTANCES = ("nodes", "edges")
# Spaces with CDF built-in schemas, which cannot be modified by users.
_COGNITE_SPACES = frozenset(
    {
        "cdf_cdm",
        "cdf_idm",
        "cdf_360_image_schema",
        "cdf_3d_schema",
        "cdf_apm",
        "cdf_apps_shared",
        "cdf_cdm_3d",
        "cdf_time_series_data",
        "cdf_cdm_units",
    }
)


_LocalContainer = tuple[ResourceRef, ContainerRequest]
_LocalView = tuple[ResourceRef, ViewRequest]
_LocalDataModel = tuple[ResourceRef, DataModelRequest]
_CountByResource = dict[str, int]
_Container = ContainerRequest | ContainerResponse


@dataclass(frozen=True)
class _ContainerPropertyReference:
    """A view property mapped onto a container property."""

    container_id: ContainerId
    property_identifier: str
    resources: tuple[ResourceRef, ...]

    @property
    def label(self) -> str:
        return f"{self.container_id}.{self.property_identifier}"


@dataclass(frozen=True)
class _ReverseDirectRelationReference:
    """A reverse direct relation pointing at a property through a view or container."""

    through: ContainerDirectId | ViewDirectId
    resources: tuple[ResourceRef, ...]


@dataclass(frozen=True)
class _SchemaReferences:
    container_properties: tuple[_ContainerPropertyReference, ...]
    reverse_direct_relations: tuple[_ReverseDirectRelationReference, ...]


class DataModelingLimits(BaseModel):
    """The ``data_modeling.*`` project limits from the limits API, by name without the prefix."""

    max_container_properties: int
    max_containers: int
    max_data_models: int
    max_deleted_instances: int
    max_instances: int
    max_records_container_properties: int
    max_records_view_properties: int
    max_spaces: int
    max_total_container_properties: int
    max_view_properties: int
    max_views: int
    max_views_per_data_model: int

    @classmethod
    def from_responses(cls, limits: Sequence[LimitValueResponse]) -> Self:
        # Pydantic accepts whole floats such as 100.0 for the int fields and rejects fractional ones.
        return cls.model_validate(
            {item.limit_id.removeprefix(_DATA_MODELING_LIMIT_PREFIX): item.value for item in limits}
        )


def _count_phrase(count_by_resource: _CountByResource) -> str:
    """For example, '1 node and 2 edges'. Resource names are plural, such as 'nodes' and 'data models'."""
    return humanize_collection(
        [
            f"{count} {name.removesuffix('s') if count == 1 else name}"
            for name, count in count_by_resource.items()
            if count
        ],
        sort=False,
    )


def _source_file(ref: ResourceRef) -> Path:
    if not isinstance(ref.source, Path):
        raise ValueError(f"Expected a local resource, got {ref.source!r} for {quote_identifier(ref.identifier)}")
    return ref.source


def _exceeded_limit(ref: ResourceRef, message: str, fix: str, keys: Sequence[str] = ()) -> BuildError:
    return with_position(
        BuildError(
            code="EXCEEDED-LIMIT",
            message=message,
            fix=fix,
            source_file=_source_file(ref),
            alpha=Flags.ALPHA_RULES.is_enabled(),
        ),
        keys=keys,
        values=identifier_values(ref.identifier),
        variables=ref.variables,
    )


class DataModelingRuleSet(ToolkitRuleSet):
    """Validates view-to-container mappings, reverse direct relations, state changes, and project limits.

    Container properties referenced by views, and the direct relations behind reverse relations, are
    resolved against local modules first. Anything still missing is checked in CDF when a client is
    available. Property and per-resource limits are checked from the local configuration. Project
    capacity for spaces, views, and similar counts is checked when deployment results are available.
    Cycles in view implements are errors, while missing requires constraints and missing indexes for
    reverse direct relations are recommendations for query performance.
    """

    RESOURCE_TYPES = frozenset(
        {
            SpaceIO.as_resource_type(),
            DataModelIO.as_resource_type(),
            ContainerIO.as_resource_type(),
            ViewIO.as_resource_type(),
        }
    )
    CONTEXT = frozenset({"local"})
    DISPLAY_NAME = "Data modeling"

    def __init__(
        self,
        resources: Sequence[ResourceRef],
        results: Sequence[DeploymentResult] | None = None,
        client: ToolkitClient | None = None,
    ) -> None:
        super().__init__(resources, results, client)
        self._local_by_io: dict[type[Any], dict[Any, tuple[ResourceRef, Any]]] = {}
        self._limits: DataModelingLimits | None = None
        self._cdf_containers: dict[ContainerId, ContainerResponse] = {}
        self._requested_container_ids: set[ContainerId] = set()
        self._cdf_views: dict[ViewId, ViewResponse] = {}
        self._requested_view_ids: set[ViewId] = set()

    def get_status(self) -> RuleSetStatus:
        if self.client is None:
            return RuleSetStatus(
                code="reduced",
                message=(
                    "No client provided, will only validate data modeling references between resources "
                    "within the provided modules, but not validate against CDF."
                ),
            )
        if not self.results:
            return RuleSetStatus(
                code="ready",
                message=(
                    "Will validate data modeling references, state changes, and property limits against CDF. "
                    "Project capacity is not checked, since no deployment results are available."
                ),
            )
        return RuleSetStatus(
            code="ready",
            message=(
                "Will validate data modeling references, state changes, property limits, "
                "and project capacity against CDF."
            ),
        )

    def validate(self) -> Iterable[Insight | InternalValidatorException]:
        yield from self._check_schema_references()
        if Flags.ALPHA_RULES.is_enabled():
            yield from self._check_schema_design()
        if self.client is None:
            return
        yield from self._validate_data_modeling_changes(self.client)
        yield from self._check_data_modeling_limits(self.client)
        if self.results:
            yield from self._check_data_modeling_capacity(self.client, self.results)

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

    def _check_schema_design(self) -> Iterable[Insight | InternalValidatorException]:
        """Checks the implements graph of views, and the requires constraints and indexes needed for queries."""
        try:
            local_containers = self._load_containers()
            local_views = self._load_views()
        except Exception:
            # Already reported by the schema reference check.
            return
        cycles = self._implements_cycles(local_views)
        yield from self._check_implements_cycles(cycles, local_views)
        in_cycle = {view_id for cycle in cycles for view_id in cycle}
        yield from self._check_requires_constraints(local_containers, local_views, skip=in_cycle)
        yield from self._check_reverse_relation_indexes(local_containers, local_views)

    @staticmethod
    def _check_implements_cycles(
        cycles: list[tuple[ViewId, ...]], local_views: dict[ViewId, _LocalView]
    ) -> Iterable[Insight]:
        for cycle in cycles:
            resource, _ = local_views[cycle[0]]
            path = " -> ".join(str(view_id) for view_id in (*cycle, cycle[0]))
            yield with_position(
                BuildError(
                    code="CYCLIC-IMPLEMENTS",
                    message=f"Detected a cycle of view implements: {path}.",
                    fix="Remove one of the implements in the cycle. Views cannot inherit from themselves.",
                    source_file=_source_file(resource),
                    alpha=Flags.ALPHA_RULES.is_enabled(),
                ),
                keys=["implements"],
                variables=resource.variables,
            )

    @classmethod
    def _implements_cycles(cls, local_views: dict[ViewId, _LocalView]) -> list[tuple[ViewId, ...]]:
        """Cycles in the implements graph of the local views, each starting at its lowest view ID.

        Non-local views are not followed, as views deployed to CDF cannot form a cycle.
        """
        cycles: list[tuple[ViewId, ...]] = []
        visited: set[ViewId] = set()
        for view_id in sorted(local_views, key=str):
            cls._find_cycles(view_id, local_views, visited, [], cycles)
        return cycles

    @classmethod
    def _find_cycles(
        cls,
        view_id: ViewId,
        local_views: dict[ViewId, _LocalView],
        visited: set[ViewId],
        path: list[ViewId],
        cycles: list[tuple[ViewId, ...]],
    ) -> None:
        """Depth-first search through the implements graph, collecting cycles reachable from view_id.

        `path` is the current chain of views from the search root; a view already in `path` closes a cycle.
        Views are appended before visiting their parents and popped afterwards (backtracking), so `path`
        only reflects the current branch. `visited` persists across branches so each view is expanded once.
        Cycles are rotated to start at their lowest view ID to avoid duplicates.
        """
        if view_id in path:
            cycle = path[path.index(view_id) :]
            start = cycle.index(min(cycle, key=str))
            rotated = (*cycle[start:], *cycle[:start])
            if rotated not in cycles:
                cycles.append(rotated)
            return
        if view_id in visited or view_id not in local_views:
            return
        visited.add(view_id)
        path.append(view_id)
        _, view_request = local_views[view_id]
        for parent_id in view_request.implements or []:
            cls._find_cycles(parent_id, local_views, visited, path, cycles)
        path.pop()

    def _check_requires_constraints(
        self,
        local_containers: dict[ContainerId, _LocalContainer],
        local_views: dict[ViewId, _LocalView],
        skip: set[ViewId],
    ) -> Iterable[Insight | InternalValidatorException]:
        """Recommends requires constraints from the containers of a view to the containers of the views it implements.

        Queries through a view are optimized when one of its containers requires, directly or transitively,
        all the other containers the view maps to.
        """
        ancestors_by_view = {
            view_id: self._view_precedence(view_id, local_views)[1:]
            for view_id, (_, view_request) in local_views.items()
            if view_request.implements and view_id not in skip
        }
        remote_ancestor_ids = {
            ancestor_id
            for ancestors in ancestors_by_view.values()
            for ancestor_id in ancestors
            if ancestor_id not in local_views
        }
        cdf_views, view_error = self._retrieve_views(remote_ancestor_ids)
        if view_error is not None:
            yield view_error

        for view_id, ancestors in ancestors_by_view.items():
            resource, view_request = local_views[view_id]
            own_containers = self._mapped_containers(view_request)
            parent_containers: set[ContainerId] = set()
            for ancestor_id in ancestors:
                if ancestor_id in local_views:
                    parent_containers |= self._mapped_containers(local_views[ancestor_id][1])
                elif ancestor_id in cdf_views:
                    parent_containers.update(cdf_views[ancestor_id].mapped_containers)
            sources = {
                container_id
                for container_id in own_containers - parent_containers
                if container_id in local_containers and not contains_unresolved_variable(container_id)
            }
            targets = parent_containers - own_containers
            if not sources or not targets:
                continue
            reachable, unresolved, error = self._required_containers(sources, local_containers)
            if error is not None:
                yield error
            missing = targets - reachable
            if not missing or unresolved:
                # Unresolved containers may require the missing ones.
                continue
            yield self._missing_requires_recommendation(
                resource, view_id, sources, self._minimal_requires(missing, local_containers)
            )

    @staticmethod
    def _mapped_containers(view: ViewRequest) -> set[ContainerId]:
        return {
            prop.container for prop in (view.properties or {}).values() if isinstance(prop, ViewCorePropertyRequest)
        }

    def _required_containers(
        self, starts: set[ContainerId], local_containers: dict[ContainerId, _LocalContainer]
    ) -> tuple[set[ContainerId], set[ContainerId], InternalValidatorException | None]:
        """The containers required, directly or transitively, by the start containers, including the start containers.

        Returns the reachable containers, the reachable containers that were not found locally or in CDF, and any
        error from retrieving containers from CDF.
        """
        reachable = set(starts)
        unresolved: set[ContainerId] = set()
        first_error: InternalValidatorException | None = None
        frontier = set(starts)
        while frontier:
            cdf_containers, error = self._retrieve_containers(frontier - set(local_containers))
            first_error = first_error or error
            next_frontier: set[ContainerId] = set()
            for container_id in frontier:
                container: _Container | None = (
                    local_containers[container_id][1]
                    if container_id in local_containers
                    else cdf_containers.get(container_id)
                )
                if container is None:
                    unresolved.add(container_id)
                    continue
                for constraint in (container.constraints or {}).values():
                    if isinstance(constraint, RequiresConstraintDefinition) and constraint.require not in reachable:
                        reachable.add(constraint.require)
                        next_frontier.add(constraint.require)
            frontier = next_frontier
        return reachable, unresolved, first_error

    def _minimal_requires(
        self, missing: set[ContainerId], local_containers: dict[ContainerId, _LocalContainer]
    ) -> list[ContainerId]:
        """The missing containers that are not already required by another missing container."""
        required_by = {
            container_id: self._required_containers({container_id}, local_containers)[0] - {container_id}
            for container_id in missing
        }
        minimal = [
            container_id
            for container_id in missing
            if not any(container_id in required_by[other] for other in missing if other != container_id)
        ]
        return sorted(minimal or missing, key=str)

    @staticmethod
    def _missing_requires_recommendation(
        resource: ResourceRef, view_id: ViewId, sources: set[ContainerId], to_require: list[ContainerId]
    ) -> Recommendation:
        source_names = [f"'{container_id}'" for container_id in sorted(sources, key=str)]
        source_text = (
            f"container {source_names[0]}"
            if len(source_names) == 1
            else f"one of the containers {humanize_collection(source_names, bind_word='or', sort=False)}"
        )
        required = humanize_collection([f"'{container_id}'" for container_id in to_require], sort=False)
        constraints = "a 'requires' constraint" if len(to_require) == 1 else "'requires' constraints"
        return with_position(
            Recommendation(
                code="MISSING-REQUIRES-CONSTRAINT",
                message=(
                    f"View '{view_id}' is not optimized for querying. None of its own containers require "
                    f"{required}, mapped by the views it implements."
                ),
                fix=(
                    f"Add {constraints} from {source_text} to {required}. Note that instances "
                    f"must then exist in {required} before they can be written to the requiring container."
                ),
                source_file=_source_file(resource),
                alpha=Flags.ALPHA_RULES.is_enabled(),
            ),
            keys=["implements"],
            variables=resource.variables,
        )

    def _check_reverse_relation_indexes(
        self, local_containers: dict[ContainerId, _LocalContainer], local_views: dict[ViewId, _LocalView]
    ) -> Iterable[Insight | InternalValidatorException]:
        """Recommends a cursorable B-tree index on the direct relations that reverse direct relations go through."""
        relations = [
            relation
            for relation in self._find_references(local_views).reverse_direct_relations
            if not contains_unresolved_variable(relation.through)
        ]
        needed_view_ids = {
            relation.through: self._reverse_fetch_ids(relation.through, local_views)[1] for relation in relations
        }
        cdf_views, view_error = self._retrieve_views(set().union(*needed_view_ids.values()))
        if view_error is not None:
            yield view_error

        targets: list[tuple[_ReverseDirectRelationReference, ContainerId, str]] = []
        for relation in relations:
            if needed_view_ids[relation.through] - set(cdf_views):
                continue
            target = self._through_container_property(relation.through, local_views, cdf_views)
            if target is not None and target[0].space not in _COGNITE_SPACES:
                targets.append((relation, *target))

        remote_ids = {
            container_id
            for _, container_id, property_id in targets
            if property_id
            not in (local_containers[container_id][1].properties if container_id in local_containers else {})
        }
        cdf_containers, container_error = self._retrieve_containers(remote_ids)
        if container_error is not None:
            yield container_error

        for relation, container_id, property_id in targets:
            container = self._container_with_property(container_id, property_id, local_containers, cdf_containers)
            if container is None:
                continue
            property_type = container.properties[property_id].type
            # Reverse relations through a list of direct relations are traversed with search, which indexes do not help.
            if not isinstance(property_type, DirectNodeRelation) or property_type.list:
                continue
            if self._has_cursorable_index(container, property_id):
                continue
            yield from self._missing_index_recommendations(relation, container_id, property_id)

    def _through_container_property(
        self,
        through: ContainerDirectId | ViewDirectId,
        local_views: dict[ViewId, _LocalView],
        cdf_views: dict[ViewId, ViewResponse],
    ) -> tuple[ContainerId, str] | None:
        if isinstance(through, ContainerDirectId):
            return through.source, through.identifier
        prop = self._find_view_property(through.source, through.identifier, local_views, cdf_views)
        if isinstance(prop, ViewCorePropertyRequest | ViewCorePropertyResponse):
            return prop.container, prop.container_property_identifier
        return None

    @staticmethod
    def _container_with_property(
        container_id: ContainerId,
        property_identifier: str,
        local_containers: dict[ContainerId, _LocalContainer],
        cdf_containers: dict[ContainerId, ContainerResponse],
    ) -> _Container | None:
        local = local_containers.get(container_id)
        if local is not None and property_identifier in local[1].properties:
            return local[1]
        cdf_container = cdf_containers.get(container_id)
        if cdf_container is not None and property_identifier in cdf_container.properties:
            return cdf_container
        return None

    @staticmethod
    def _has_cursorable_index(container: _Container, property_identifier: str) -> bool:
        return any(
            isinstance(index, BtreeIndex) and index.properties == [property_identifier] and index.cursorable
            for index in (container.indexes or {}).values()
        )

    def _missing_index_recommendations(
        self, relation: _ReverseDirectRelationReference, container_id: ContainerId, property_identifier: str
    ) -> Iterable[Insight]:
        for resource in relation.resources:
            yield with_position(
                Recommendation(
                    code="MISSING-REVERSE-RELATION-INDEX",
                    message=(
                        f"The reverse direct relation through '{relation.through}' in "
                        f"{self._referenced_by(resource)} cannot be queried efficiently, since the direct relation "
                        f"'{container_id}.{property_identifier}' has no cursorable B-tree index."
                    ),
                    fix=(
                        f"Add a B-tree index with 'cursorable: true' on property '{property_identifier}' "
                        f"in container '{container_id}'."
                    ),
                    source_file=_source_file(resource),
                    alpha=Flags.ALPHA_RULES.is_enabled(),
                ),
                values=[relation.through.identifier],
                variables=resource.variables,
            )

    @classmethod
    def _find_references(cls, local_views: dict[ViewId, _LocalView]) -> _SchemaReferences:
        container_properties: dict[tuple[ContainerId, str], list[ResourceRef]] = defaultdict(list)
        reverse_relations: dict[ContainerDirectId | ViewDirectId, list[ResourceRef]] = defaultdict(list)
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
            _, needed_view_ids = self._reverse_fetch_ids(relation_ref.through, local_views)
            if needed_view_ids:
                # Non-local views may override the local definition, so this can only be resolved with CDF.
                missing_reverses.append(relation_ref)
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
            needed_container_ids, needed_view_ids = self._reverse_fetch_ids(relation_ref.through, local_views)
            if (needed_container_ids and container_error is not None) or (needed_view_ids and view_error is not None):
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
            reverse_container_ids, reverse_view_ids = self._reverse_fetch_ids(ref.through, local_views)
            container_ids.update(reverse_container_ids)
            view_ids.update(reverse_view_ids)
        return container_ids, view_ids

    @classmethod
    def _reverse_fetch_ids(
        cls,
        through: ContainerDirectId | ViewDirectId,
        local_views: dict[ViewId, _LocalView],
    ) -> tuple[set[ContainerId], set[ViewId]]:
        """The containers or views that must be fetched from CDF to resolve a still-missing reverse relation.

        Only the non-local views in the implements chain are fetched. CDF expands their own inheritance.
        """
        if isinstance(through, ContainerDirectId):
            return {through.source}, set()
        # Non-local views earlier in the precedence order may override a later local definition,
        # so they must be fetched even when the property is found locally further down.
        higher_precedence_remote_ids: set[ViewId] = set()
        for view_id in cls._view_precedence(through.source, local_views):
            local = local_views.get(view_id)
            if local is None:
                higher_precedence_remote_ids.add(view_id)
                continue
            _, view_request = local
            prop = (view_request.properties or {}).get(through.identifier)
            if isinstance(prop, ViewCorePropertyRequest):
                return {prop.container}, higher_precedence_remote_ids
            if prop is not None:
                return set(), higher_precedence_remote_ids
        return set(), higher_precedence_remote_ids

    @classmethod
    def _view_precedence(cls, view_id: ViewId, local_views: dict[ViewId, _LocalView]) -> list[ViewId]:
        """The view followed by its ancestors, highest property precedence first.

        The last implemented view wins, and each parent's own ancestors are ranked right after it. For a view
        implementing [C, D], the order is the view, D and D's ancestors, then C and C's ancestors. A view reachable
        several times (diamond) keeps its first, highest-precedence, position. Follows implements through local
        views only; non-local views are kept as leaves.
        """
        precedence: list[ViewId] = []
        cls._visit_view(view_id, local_views, set(), precedence)
        return precedence

    @classmethod
    def _visit_view(
        cls,
        view_id: ViewId,
        local_views: dict[ViewId, _LocalView],
        visited: set[ViewId],
        precedence: list[ViewId],
    ) -> None:
        if view_id in visited:
            return
        visited.add(view_id)
        precedence.append(view_id)
        local = local_views.get(view_id)
        if local is not None:
            _, view_request = local
            for parent_id in reversed(view_request.implements or []):
                cls._visit_view(parent_id, local_views, visited, precedence)

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

    @classmethod
    def _find_view_property(
        cls,
        view_id: ViewId,
        property_identifier: str,
        local_views: dict[ViewId, _LocalView],
        cdf_views: dict[ViewId, ViewResponse],
    ) -> ViewRequestProperty | ViewResponseProperty | None:
        for candidate_id in cls._view_precedence(view_id, local_views):
            prop: ViewRequestProperty | ViewResponseProperty | None = None
            local = local_views.get(candidate_id)
            if local is not None:
                _, view_request = local
                prop = (view_request.properties or {}).get(property_identifier)
            elif (cdf_view := cdf_views.get(candidate_id)) is not None:
                prop = cdf_view.properties.get(property_identifier)
            if prop is not None:
                return prop
        return None

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
        return self._load_local(ContainerIO(self._load_client()))

    def _load_views(self) -> dict[ViewId, _LocalView]:
        return self._load_local(ViewIO(self._load_client()))

    def _load_data_models(self) -> dict[DataModelId, _LocalDataModel]:
        return self._load_local(DataModelIO(self._load_client()))

    def _load_local(
        self,
        crud: ResourceIO[_resource_base.T_Identifier, _resource_base.T_RequestResource, Any, Any],
    ) -> dict[_resource_base.T_Identifier, tuple[ResourceRef, _resource_base.T_RequestResource]]:
        """Loads the local resources of the CRUD's type, the same way ``cdf deploy`` does. Cached per CRUD type."""
        crud_type = type(crud)
        if crud_type not in self._local_by_io:
            local_by_id: dict[Any, tuple[ResourceRef, Any]] = {}
            for resource, validatable in self.resources.get_refs_and_resource_of_type(crud):
                if isinstance(resource.source, Path):
                    local_by_id[crud.get_id(validatable.request)] = (resource, validatable.request)
            self._local_by_io[crud_type] = local_by_id
        return cast(
            dict[_resource_base.T_Identifier, tuple[ResourceRef, _resource_base.T_RequestResource]],
            self._local_by_io[crud_type],
        )

    def _retrieve_containers(
        self, container_ids: set[ContainerId]
    ) -> tuple[dict[ContainerId, ContainerResponse], InternalValidatorException | None]:
        """Retrieves containers from CDF. Each container is requested at most once, so a failure is only reported
        to the first caller."""
        if not container_ids or self.client is None:
            return {}, None
        to_request = container_ids - self._requested_container_ids
        self._requested_container_ids.update(to_request)
        error: InternalValidatorException | None = None
        if to_request:
            try:
                items = ContainerIO(self.client).retrieve(list(to_request))
            except ToolkitAPIError as e:
                error = InternalValidatorException(
                    message=f"Failed to verify existence of {ContainerIO.kind.lower()} in CDF: {e}",
                    source=ContainerIO.kind,
                )
            else:
                self._cdf_containers.update({item.as_id(): item for item in items})
        return {
            container_id: self._cdf_containers[container_id]
            for container_id in container_ids
            if container_id in self._cdf_containers
        }, error

    def _retrieve_views(
        self, view_ids: set[ViewId]
    ) -> tuple[dict[ViewId, ViewResponse], InternalValidatorException | None]:
        """Retrieves views, with inherited properties, from CDF. Each view is requested at most once, so a failure
        is only reported to the first caller."""
        if not view_ids or self.client is None:
            return {}, None
        to_request = view_ids - self._requested_view_ids
        self._requested_view_ids.update(to_request)
        error: InternalValidatorException | None = None
        if to_request:
            try:
                items = self.client.views.retrieve(list(to_request), include_inherited_properties=True)
            except ToolkitAPIError as e:
                error = InternalValidatorException(
                    message=f"Failed to verify existence of {ViewIO.kind.lower()} in CDF: {e}",
                    source=ViewIO.kind,
                )
            else:
                self._cdf_views.update({item.as_id(): item for item in items})
        return {view_id: self._cdf_views[view_id] for view_id in view_ids if view_id in self._cdf_views}, error

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
                        source_file=_source_file(resource),
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
                        source_file=_source_file(resource),
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
                    source_file=_source_file(resource),
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
                    source_file=_source_file(resource),
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
                    source_file=_source_file(resource),
                ),
                values=[ref.through.identifier],
                variables=resource.variables,
            )

    @staticmethod
    def _referenced_by(resource: ResourceRef) -> str:
        return quote_identifier(resource.identifier)

    @staticmethod
    def _append(bucket: dict[_KeyT, list[ResourceRef]], key: _KeyT, resource: ResourceRef) -> None:
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
            local_by_id = self._load_local(crud)
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
        resource: ResourceRef,
        request: Any,
        cdf_item: Any,
    ) -> Iterable[Insight]:
        if isinstance(item_id, ContainerId):
            assert isinstance(request, ContainerRequest) and isinstance(cdf_item, ContainerResponse)
            yield from self._container_insights(item_id, _source_file(resource), request, cdf_item)
        elif isinstance(item_id, ViewId):
            assert isinstance(request, ViewRequest) and isinstance(cdf_item, ViewResponse)
            yield from self._view_insights(item_id, _source_file(resource), request, cdf_item)
        elif isinstance(item_id, DataModelId):
            assert isinstance(request, DataModelRequest) and isinstance(cdf_item, DataModelResponse)
            yield from self._data_model_insights(item_id, _source_file(resource), request, cdf_item)

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

    def _check_data_modeling_limits(self, client: ToolkitClient) -> Iterable[Insight | InternalValidatorException]:
        """Checks property and per-resource data modeling limits from the local configuration."""
        try:
            containers = self._load_containers()
            views = self._load_views()
            data_models = self._load_data_models()
        except Exception as error:
            yield InternalValidatorException(
                message=f"Failed to load local data modeling resources for limit checks: {error}",
                source="DataModeling",
            )
            return
        if not (containers or views or data_models):
            return
        limits, limits_error = self._retrieve_limits(client)
        if limits is None:
            if limits_error is not None:
                yield limits_error
            return
        yield from self._check_local_limits(limits, containers, views, data_models)

    def _check_data_modeling_capacity(
        self, client: ToolkitClient, results: Sequence[DeploymentResult]
    ) -> Iterable[Insight | InternalValidatorException]:
        """Checks that the project capacity (how many spaces, views, instances, and so on would exist
        after deploy) stays within the data modeling limits.

        Deployment results are the only way to know what would be created and deleted.
        """
        limits, limits_error = self._retrieve_limits(client)
        if limits is None:
            if limits_error is not None:
                yield limits_error
            return
        try:
            statistics = client.statistics.retrieve()
        except ToolkitAPIError as error:
            yield InternalValidatorException(
                message=f"Failed to retrieve data modeling statistics: {error}", source="DataModeling"
            )
            return
        yield from self._check_project_capacity(client, limits, statistics, results)

    def _retrieve_limits(
        self, client: ToolkitClient
    ) -> tuple[DataModelingLimits | None, InternalValidatorException | None]:
        if self._limits is not None:
            return self._limits, None
        try:
            self._limits = DataModelingLimits.from_responses(
                client.limits.list(limit=None, prefix=_DATA_MODELING_LIMIT_PREFIX)
            )
        except ToolkitAPIError as error:
            return None, InternalValidatorException(
                message=f"Failed to retrieve data modeling limits: {error}", source="DataModeling"
            )
        except ValidationError as error:
            return None, InternalValidatorException(
                message=f"Failed to parse data modeling limits: {error}", source="DataModeling"
            )
        return self._limits, None

    def _check_local_limits(
        self,
        limits: DataModelingLimits,
        containers: dict[ContainerId, _LocalContainer],
        views: dict[ViewId, _LocalView],
        data_models: dict[DataModelId, _LocalDataModel],
    ) -> Iterable[Insight | InternalValidatorException]:
        for ref, container in containers.values():
            if container.used_for == "record":
                kind, limit = "record container", limits.max_records_container_properties
            else:
                kind, limit = "container", limits.max_container_properties
            yield from self._check_resource_limit(ref, kind, len(container.properties), "properties", limit)
        for ref, view in views.values():
            if view.stream_id:
                kind, limit = "record view", limits.max_records_view_properties
            else:
                kind, limit = "view", limits.max_view_properties
            yield from self._check_resource_limit(ref, kind, len(view.properties or {}), "properties", limit)
        for ref, data_model in data_models.values():
            yield from self._check_resource_limit(
                ref, "data model", len(data_model.views or []), "views", limits.max_views_per_data_model
            )

    @staticmethod
    def _check_resource_limit(ref: ResourceRef, kind: str, count: int, noun: str, limit: int) -> Iterable[Insight]:
        if count <= limit:
            return
        yield _exceeded_limit(
            ref,
            (
                f"{kind.capitalize()} {quote_identifier(ref.identifier)} has {count} {noun}, "
                f"which exceeds the limit of {limit}."
            ),
            f"Remove {noun} until the {kind} has at most {limit}.",
            keys=[noun],
        )

    def _check_project_capacity(
        self,
        client: ToolkitClient,
        limits: DataModelingLimits,
        statistics: ProjectStatisticsResponse,
        results: Sequence[DeploymentResult],
    ) -> Iterable[Insight]:
        checks: list[tuple[str, str, int, int, Sequence[str] | None]] = [
            ("spaces", "/models/spaces", statistics.spaces.count, limits.max_spaces, None),
            ("containers", "/models/containers", statistics.containers.count, limits.max_containers, None),
            ("views", "/models/views", statistics.views.count, limits.max_views, None),
            ("data models", "/models/datamodels", statistics.data_models.count, limits.max_data_models, None),
            ("instances", _INSTANCES_ENDPOINT, statistics.instances.instances, limits.max_instances, _INSTANCES),
        ]
        for noun, endpoint, current, limit, resource_names in checks:
            yield from self._check_capacity(
                results, noun, client.config.create_api_url(endpoint), current, limit, resource_names
            )
        yield from self._check_soft_deleted_capacity(
            results,
            client.config.create_api_url(_INSTANCES_ENDPOINT),
            statistics.instances.soft_deleted_instances,
            limits.max_deleted_instances,
        )

    @classmethod
    def _check_capacity(
        cls,
        results: Sequence[DeploymentResult],
        noun: str,
        endpoint_url: str,
        current: int,
        limit: int,
        resource_names: Sequence[str] | None = None,
    ) -> Iterable[Insight]:
        """Checks that deploying keeps the project total of ``noun`` within its limit.

        ``resource_names`` are the deployment results that count toward the total. Defaults to ``noun`` itself.
        """
        created, deleted = cls._deployed_counts(results, resource_names or (noun,))
        created_count = sum(created.values())
        deleted_count = sum(deleted.values())
        projected = current + created_count - deleted_count
        if projected <= current or projected <= limit:
            return
        if deleted_count:
            change = f"create {_count_phrase(created)} and delete {_count_phrase(deleted)}, changing"
        else:
            change = f"create {_count_phrase(created)}, increasing"
        yield BuildError(
            code="EXCEEDED-LIMIT",
            message=(
                f"Deploying would {change} the total number of {noun} from {current} to {projected}, "
                f"which exceeds the project limit of {limit}."
            ),
            fix=(
                f"Reduce the number of {noun} in this deployment so the project total stays within {limit}. "
                f"The project currently has {current} of {limit} {noun}."
            ),
            source_url=endpoint_url,
            alpha=Flags.ALPHA_RULES.is_enabled(),
        )

    @classmethod
    def _check_soft_deleted_capacity(
        cls, results: Sequence[DeploymentResult], endpoint_url: str, current: int, limit: int
    ) -> Iterable[Insight]:
        _, deleted = cls._deployed_counts(results, _INSTANCES)
        deleted_count = sum(deleted.values())
        projected = current + deleted_count
        if not deleted_count or projected <= limit:
            return
        yield BuildError(
            code="EXCEEDED-LIMIT",
            message=(
                f"Deploying would delete {_count_phrase(deleted)}, increasing the number of soft-deleted instances "
                f"from {current} to {projected}, which exceeds the project limit of {limit}."
            ),
            fix=(
                f"Delete fewer instances in this deployment so the number of soft-deleted instances stays within "
                f"{limit}. The project currently has {current} soft-deleted instances."
            ),
            source_url=endpoint_url,
            alpha=Flags.ALPHA_RULES.is_enabled(),
        )

    @staticmethod
    def _deployed_counts(
        results: Sequence[DeploymentResult], resource_names: Sequence[str]
    ) -> tuple[_CountByResource, _CountByResource]:
        """The number of resources that deploying would create and delete, by resource name."""
        created: _CountByResource = dict.fromkeys(resource_names, 0)
        deleted: _CountByResource = dict.fromkeys(resource_names, 0)
        for result in results:
            if result.resource_name in created:
                created[result.resource_name] += result.created_count
                deleted[result.resource_name] += result.deleted_count
        return created, deleted
