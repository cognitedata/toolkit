"""Token inspect response models.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Token/operation/inspectToken
"""

from collections import UserDict, defaultdict
from collections.abc import Callable, Sequence
from typing import Any, TypeAlias, cast

from pydantic import JsonValue, SerializationInfo, model_serializer, model_validator

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.client.resource_classes.group import (
    GroupCapability,
    GroupResponse,
    Scope,
    ScopeDefinition,
    UnknownScope,
)
from cognite_toolkit._cdf_tk.client.resource_classes.group._constants import ACL_NAME
from cognite_toolkit._cdf_tk.client.resource_classes.group.acls import (
    _KNOWN_ACLS,
    Acl,
    AclType,
    GroupsAcl,
    ProjectsAcl,
    UnknownAcl,
)
from cognite_toolkit._cdf_tk.client.resource_classes.group.scope_logic import (
    scope_difference,
    scope_union,
)
from cognite_toolkit._cdf_tk.exceptions import AuthorizationError
from cognite_toolkit._cdf_tk.utils import humanize_collection

AclName: TypeAlias = str
AclAction: TypeAlias = str


class InspectProjectInfo(BaseModelObject):
    """Project information returned by the token inspect endpoint."""

    project_url_name: str
    groups: list[int]


class AllProjects(BaseModelObject):
    """Project scope for a capability in the token inspect response.

    When ``all_projects`` is set (even as an empty dict), the capability applies
    to every project the token has access to.
    """

    all_projects: dict[str, JsonValue]


class ProjectList(BaseModelObject):
    projects: list[str]


class InspectCapability(BaseModelObject):
    """A single capability entry in the token inspect response.

    Reuses the ACL types from ``group.acls`` for the ``acl`` field.
    """

    acl: AclType
    project_scope: AllProjects | ProjectList | None = None

    @model_validator(mode="before")
    @classmethod
    def move_acl_name(cls, value: Any) -> Any:
        """Move ACL key (e.g. 'groupsAcl') into the ``acl`` field."""
        if not isinstance(value, dict):
            return value
        if "acl" in value:
            return value
        acl_name = next((key for key in value if key.endswith("Acl")), None)
        if acl_name is None:
            return value
        value_copy = value.copy()
        acl_data = dict(value_copy.pop(acl_name))
        acl_data[ACL_NAME] = acl_name
        value_copy["acl"] = acl_data
        return value_copy

    @model_serializer
    def serialize_acl_name(self, info: SerializationInfo) -> dict[str, Any]:
        """Serialize 'acl' field back to its specific ACL key (e.g., 'assetsAcl') for API compatibility."""
        acl_data = self.acl.model_dump(**vars(info))
        output: dict[str, Any] = {self.acl.acl_name: acl_data}
        if self.project_scope is not None:
            output["projectScope" if info.by_alias else "project_scope"] = self.project_scope.model_dump(**vars(info))
        return output


def _collapse_scopes(scopes: list[Scope]) -> list[Scope]:
    """Union scopes that share a type.

    Different scope types are kept as separate entries. Returns None when an unknown scope cannot
    be combined, so those capabilities are left out of the flat map.
    """
    if len(scopes) <= 1:
        return list(scopes)
    try:
        return [scope_union(*scopes)]
    except ValueError:
        pass
    except TypeError:
        if any(isinstance(scope, UnknownScope) for scope in scopes):
            return scopes
        raise

    grouped: dict[tuple[type[ScopeDefinition], str], list[Scope]] = {}
    for scope in scopes:
        grouped.setdefault((type(scope), scope.scope_name), []).append(scope)

    collapsed: list[Scope] = []
    for group in grouped.values():
        if len(group) == 1:
            collapsed.append(group[0])
            continue
        try:
            collapsed.append(scope_union(*group))
        except TypeError:
            if any(isinstance(scope, UnknownScope) for scope in group):
                # Unknown scopes with unhashable fields cannot be combined, and are never required
                # when verifying capabilities.
                collapsed.extend(group)  # keep instead of dropping
                continue
            raise
    return collapsed


def _uncovered_scope(required: ScopeDefinition, granted_scopes: list[Scope]) -> ScopeDefinition | None:
    """Return the part of ``required`` that none of ``granted_scopes`` covers."""
    remaining: ScopeDefinition | None = required
    for granted in granted_scopes:
        if remaining is None:
            return None
        try:
            remaining = scope_difference(remaining, granted)
        except (TypeError, ValueError):
            continue
    return remaining


class FlatCapabilities(UserDict[tuple[type[Acl], AclName, AclAction], list[Scope]]):
    """A helper class to represent the capabilities for a project and group(s)

    The capabilities are stored as a mapping from (ACL type, AclName, action) to scopes. Scopes of the
    same type are combined into one scope. Scopes of different types are kept separate, since they cannot
    be represented as a single scope. This allows for easy verification of ACLs against the capabilities.
    Note that AclName is included to account for UnknownAcls, which may have the same ACL type but different names, and thus different capabilities.
    """

    def __init__(
        self,
        capabilities: dict[tuple[type[Acl], AclName, AclAction], list[Scope]],
        name: str,
        groups: list[int],
    ) -> None:
        super().__init__(capabilities)
        self.name = name
        self.groups = groups

    def verify(self, acls: Sequence[AclType]) -> Sequence[AclType]:
        """Verify that the provided ACLs are covered by the capabilities in this project.

        Args:
            acls: The ACLs to verify.

        Returns:
            The list of ACLs that are not covered by the capabilities in this project.
        """
        missing_actions_by_type_and_scope: dict[tuple[type[AclType], AclName, ScopeDefinition], set[str]] = defaultdict(
            set
        )
        for acl in acls:
            for action in acl.actions:
                key = (type(acl), acl.acl_name, action)
                granted_scopes = self.data.get(key)
                if not granted_scopes:
                    missing_actions_by_type_and_scope[(type(acl), acl.acl_name, acl.scope)].add(action)
                    continue
                if missing_scope := _uncovered_scope(acl.scope, granted_scopes):
                    missing_actions_by_type_and_scope[(type(acl), acl.acl_name, missing_scope)].add(action)

        return self._merge_als(missing_actions_by_type_and_scope)

    @classmethod
    def merge_acls(cls, acls: list[AclType]) -> Sequence[AclType]:
        actions_by_type_and_scope: dict[tuple[type[AclType], AclName, ScopeDefinition], set[str]] = defaultdict(set)
        for acl in acls:
            actions_by_type_and_scope[(type(acl), acl.acl_name, acl.scope)].update(acl.actions)
        return cls._merge_als(actions_by_type_and_scope)

    @classmethod
    def _merge_als(
        cls, actions_by_type_and_scope: dict[tuple[type[AclType], AclName, ScopeDefinition], set[str]]
    ) -> Sequence[AclType]:
        merged_acls: list[AclType] = []
        for (acl_type, acl_name, scope), actions in actions_by_type_and_scope.items():
            merged_acls.append(
                cast(Callable[..., AclType], acl_type)(actions=sorted(actions), acl_name=acl_name, scope=scope)
            )
        return merged_acls

    @classmethod
    def from_capabilities(
        cls, capabilities: Sequence[InspectCapability | GroupCapability], project: str, groups: list[int]
    ) -> "FlatCapabilities":
        """Convert a list of capabilities to a FlatCapabilities object for a specific project.

        This method filters the capabilities for the specified project. Scopes for the same ACL and action
        are combined when they have the same type, and kept as a list when they do not.

        Args:
            capabilities: The list of capabilities to convert.
            project: The project to filter capabilities for.
            groups: The list of group IDs that the user is a member of for the specified project

        Returns:
            A FlatCapabilities object containing the capabilities for the specified project.

        """
        scopes_by_acl_action: dict[tuple[type[Acl], AclName, AclAction], list[Scope]] = defaultdict(list)
        seen_scopes: dict[tuple[type[Acl], AclName, AclAction], set[Scope]] = defaultdict(set)
        for capability in capabilities:
            if isinstance(capability, InspectCapability) and not (
                isinstance(capability.project_scope, AllProjects)
                or (isinstance(capability.project_scope, ProjectList) and project in capability.project_scope.projects)
            ):
                continue
            elif isinstance(capability, GroupCapability) and not (
                capability.project_url_names is None or project in capability.project_url_names.url_names
            ):
                continue

            for action in capability.acl.actions:
                # The type(capability.acl) can be UnknownAcl for a known Acl, if there is an action or scope
                # that is unknown. Thus, we use the acl_name instead.
                acl_type = _KNOWN_ACLS.get(capability.acl.acl_name, UnknownAcl)
                key = (acl_type, capability.acl.acl_name, action)
                scope = capability.acl.scope
                if scope in seen_scopes[key]:
                    continue
                seen_scopes[key].add(scope)
                scopes_by_acl_action[key].append(scope)

        scope_by_acl_action: dict[tuple[type[Acl], AclName, AclAction], list[Scope]] = {}
        for key, scopes in scopes_by_acl_action.items():
            scope_by_acl_action[key] = _collapse_scopes(scopes)
        return FlatCapabilities(capabilities=scope_by_acl_action, name=project, groups=groups)

    @classmethod
    def from_group(cls, group: GroupResponse, project: str) -> "FlatCapabilities":
        return cls.from_capabilities(group.capabilities or [], project, groups=[group.id])


class InspectResponse(BaseModelObject):
    """Response from the ``GET /api/v1/token/inspect`` endpoint."""

    subject: str
    projects: list[InspectProjectInfo]
    capabilities: list[InspectCapability]
    # This is not part of the API response, but we manually set it to the current project as it is very useful
    project: str = ""

    def to_project_capabilities(self, project: str | None = None) -> FlatCapabilities:
        """Convert the inspect response to a ProjectCapabilities object for easier access to ACLs by project.

        Args:
            project: The project to filter capabilities for. If None, uses the project set in the response
                (which is the current project).

        Returns:
            A ProjectCapabilities object containing the capabilities for the specified project.
        """
        project = project or self.project
        project_info = next((p for p in self.projects if p.project_url_name == project), None)
        if project_info is None:
            available_projects = [p.project_url_name for p in self.projects]
            if available_projects:
                suffix = f" Available projects: {humanize_collection(available_projects)}."
            else:
                required_capabilities = f"{ProjectsAcl.__name__} and {GroupsAcl.__name__} capabilities with LIST action"
                suffix = f" You are likely not a member of a group with {required_capabilities}."
            raise AuthorizationError(f"Missing project '{project}' in inspect response.{suffix}")
        return FlatCapabilities.from_capabilities(
            capabilities=self.capabilities, project=project, groups=project_info.groups
        )
