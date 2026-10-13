"""Syntax and cross-resource validation for the modeling sandbox."""

import re
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

from pydantic import ValidationError

from cognite_toolkit._cdf_tk.client._resource_base import Identifier, RequestResource
from cognite_toolkit._cdf_tk.client.identifiers import ContainerId, ExternalId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import ContainerRequest, ViewRequest
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuiltResource, contains_unresolved_variable
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import (
    BuildError,
    BuildWarning,
    ConsistencyError,
    FileReadError,
    Insight,
    ModelSyntaxError,
)
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import BuildVariable, ModuleId
from cognite_toolkit._cdf_tk.resource_ios import ContainerIO, ResourceIO, ResourceType, ViewIO
from cognite_toolkit._cdf_tk.rules._base import InternalValidatorException
from cognite_toolkit._cdf_tk.rules._data_modeling import (
    DataModelingRuleSet,
    _ContainerPropertyReference,
    _LocalContainer,
    _LocalView,
    _SchemaReferences,
)
from cognite_toolkit._cdf_tk.sandbox._source import SqlSource

BLOCKING_INSIGHT_TYPES = (ModelSyntaxError, ConsistencyError, BuildError, FileReadError)

# Inherited properties of the CDM views that data models most often implement.
_CDM_PROPERTIES: dict[str, set[str]] = {
    "CogniteDescribable": {"name", "description", "tags", "aliases"},
    "CogniteSourceable": {
        "sourceId",
        "sourceContext",
        "source",
        "sourceCreatedTime",
        "sourceUpdatedTime",
        "sourceCreatedUser",
        "sourceUpdatedUser",
    },
}
_NODE_FIELDS = {"externalId", "space"}


@dataclass(frozen=True)
class ResourceLocation:
    """Where a request lives in the session."""

    path: Path
    crud: ResourceIO
    variables: list[BuildVariable]


def is_blocking_insight(insight: Insight | InternalValidatorException) -> bool:
    return isinstance(insight, BLOCKING_INSIGHT_TYPES)


def syntax_insights(
    request: RequestResource,
    crud: ResourceIO,
    source_path: Path,
) -> list[Insight]:
    """Run yaml_cls validation on a request dump."""
    try:
        crud.validate_object(request.dump(), extra="forbid")
    except ValidationError as error:
        return [
            ModelSyntaxError(
                code="MODEL-SYNTAX-ERROR",
                message=str(error),
                source_file=source_path.resolve(),
                fix="Fix the resource so it matches the Toolkit YAML schema.",
            )
        ]
    return []


def _stand_in_built_resource(
    identifier: Identifier,
    source_path: Path,
    crud: type[ResourceIO],
    variables: list[BuildVariable],
) -> BuiltResource:
    """Minimal BuiltResource so DataModelingRuleSet can attach insights to a path."""
    module_id = ModuleId(id=Path("sandbox"), path=source_path.parent)
    resource_type = ResourceType(resource_folder=crud.folder_name, kind=crud.kind)
    return BuiltResource(
        identifier=identifier,
        source_hash="sandbox",
        type=resource_type,
        source_path=source_path.resolve(),
        build_path=source_path.resolve(),
        crud_cls=crud,
        has_syntax_error=False,
        variables=variables,
        module_id=module_id,
    )


class InMemoryDataModelingRuleSet(DataModelingRuleSet):
    """DataModelingRuleSet backed by in-memory request objects instead of the build dir.

    When a container exists in the session but a referenced property does not,
    the missing property is reported as a BuildError (blocking). References to
    containers outside the session stay as BuildWarning (unverified).
    """

    def __init__(
        self,
        containers: dict[ContainerId, tuple[ResourceLocation, ContainerRequest]],
        views: dict[ViewId, tuple[ResourceLocation, ViewRequest]],
    ) -> None:
        super().__init__(modules=[], client=None)
        self._containers = containers
        self._views = views

    def _load_containers(self) -> dict[ContainerId, _LocalContainer]:
        loaded: dict[ContainerId, _LocalContainer] = {}
        for container_id, (location, request) in self._containers.items():
            resource = _stand_in_built_resource(container_id, location.path, ContainerIO, location.variables)
            loaded[container_id] = (resource, request)
        return loaded

    def _load_views(self) -> dict[ViewId, _LocalView]:
        loaded: dict[ViewId, _LocalView] = {}
        for view_id, (location, request) in self._views.items():
            resource = _stand_in_built_resource(view_id, location.path, ViewIO, location.variables)
            loaded[view_id] = (resource, request)
        return loaded

    def _load_client(self) -> Any:
        return cast(Any, SimpleNamespace(console=None))

    def _validate_references(
        self,
        references: _SchemaReferences,
        local_containers: dict[ContainerId, _LocalContainer],
        local_views: dict[ViewId, _LocalView],
    ) -> Iterable[Insight | InternalValidatorException]:
        missing_in_session: list[_ContainerPropertyReference] = []
        unknown_outside: list[_ContainerPropertyReference] = []

        for property_ref in references.container_properties:
            if contains_unresolved_variable(property_ref.label):
                continue
            if property_ref.container_id not in local_containers:
                unknown_outside.append(property_ref)
                continue
            if (
                self._container_property(
                    property_ref.container_id,
                    property_ref.property_identifier,
                    local_containers,
                    {},
                )
                is None
            ):
                missing_in_session.append(property_ref)

        for ref in missing_in_session:
            yield from self._unknown_property_errors(ref)
        yield from self._unverified_properties(unknown_outside)

        # Reverse relations without a CDF client: keep not_direct as errors, but treat
        # "missing" as unverified warnings. Inherited / external through-targets are
        # common in real projects and cannot be resolved from the session alone.
        invalid_reverses = []
        missing_reverses = []
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
        yield from self._unverified_reverses(missing_reverses)


def _select_aliases(sql: str) -> list[str]:
    """Output column names between the first SELECT and the first FROM."""
    sql = re.sub(r"--[^\n]*", "", sql)
    match = re.search(r"\bselect\b(.*?)\bfrom\b", sql, flags=re.IGNORECASE | re.DOTALL)
    if match is None:
        return []
    items: list[str] = []
    depth = 0
    current = ""
    for char in match.group(1):
        if char == "," and depth == 0:
            items.append(current)
            current = ""
            continue
        depth += {"(": 1, ")": -1}.get(char, 0)
        current += char
    items.append(current)

    aliases: list[str] = []
    for item in items:
        item = item.strip()
        if alias_match := re.search(r"\bas\s+`?(\w+)`?\s*$", item, flags=re.IGNORECASE):
            aliases.append(alias_match.group(1))
        elif column_match := re.fullmatch(r"(?:`?\w+`?\.)?`?(\w+)`?", item):
            aliases.append(column_match.group(1))
    return aliases


def _view_properties(view_id: ViewId, views: dict[ViewId, ViewRequest]) -> set[str] | None:
    """Own and inherited property names, or None when a parent view is unknown."""
    view = views.get(view_id)
    if view is None:
        return None
    names = set(view.properties or {})
    for parent in view.implements or []:
        if parent in views:
            inherited = _view_properties(parent, views)
        elif parent.space == "cdf_cdm" and parent.external_id in _CDM_PROPERTIES:
            inherited = _CDM_PROPERTIES[parent.external_id]
        else:
            return None
        if inherited is None:
            return None
        names |= inherited
    return names


def transformation_alias_insights(
    queries: dict[ExternalId, SqlSource], views: dict[ViewId, ViewRequest]
) -> list[Insight]:
    """Warn when a transformation selects a column its destination view has no property for."""
    insights: list[Insight] = []
    for source in queries.values():
        if source.destination_view is None:
            continue
        known = _view_properties(source.destination_view, views)
        if known is None:
            continue
        for alias in _select_aliases(source.resolved_text):
            if alias in known or alias in _NODE_FIELDS:
                continue
            insights.append(
                BuildWarning(
                    code="MISSING-REFERENCED-PROPERTY",
                    message=(
                        f"Transformation '{source.transformation.external_id}' selects '{alias}', "
                        f"which is not a property of its destination view {source.destination_view}."
                    ),
                    fix=f"Add '{alias}' to the view, or remove it from the select list.",
                    source_file=source.path,
                )
            )
    return insights


def validate_session(
    requests: dict[Identifier, RequestResource],
    locations: dict[Identifier, ResourceLocation],
    queries: dict[ExternalId, SqlSource] | None = None,
    *,
    syntax_ids: set[Identifier] | None = None,
) -> list[Insight | InternalValidatorException]:
    """Run syntax validation and in-memory reference checks.

    Args:
        requests: All resources in the session.
        locations: Source locations for those resources.
        queries: Transformation SQL files in the session.
        syntax_ids: If set, only these identifiers are syntax-checked. Reference
            checks always run across the full session. Existing authored YAML is
            often looser than a request dump + yaml_cls forbid round-trip, so
            apply() should pass only dirty/new ids here.
    """
    insights: list[Insight | InternalValidatorException] = []
    for identifier, request in requests.items():
        if syntax_ids is not None and identifier not in syntax_ids:
            continue
        location = locations[identifier]
        insights.extend(syntax_insights(request, location.crud, location.path))

    containers: dict[ContainerId, tuple[ResourceLocation, ContainerRequest]] = {}
    views: dict[ViewId, tuple[ResourceLocation, ViewRequest]] = {}
    for identifier, request in requests.items():
        if isinstance(identifier, ContainerId) and isinstance(request, ContainerRequest):
            containers[identifier] = (locations[identifier], request)
        elif isinstance(identifier, ViewId) and isinstance(request, ViewRequest):
            views[identifier] = (locations[identifier], request)

    rule_set = InMemoryDataModelingRuleSet(containers=containers, views=views)
    insights.extend(list(rule_set.validate()))
    insights.extend(
        transformation_alias_insights(queries or {}, {view_id: view for view_id, (_, view) in views.items()})
    )
    return insights
