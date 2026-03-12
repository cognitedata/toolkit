"""Token inspect response models.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Token/operation/inspectToken
"""

from collections import UserDict, defaultdict
from collections.abc import Sequence
from typing import Any, TypeAlias

from cognite.client.data_classes.capabilities import UnknownScope
from pydantic import JsonValue, model_validator

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.client.resource_classes.group import Scope, ScopeDefinition
from cognite_toolkit._cdf_tk.client.resource_classes.group._constants import ACL_NAME
from cognite_toolkit._cdf_tk.client.resource_classes.group.acls import Acl, AclType
from cognite_toolkit._cdf_tk.client.resource_classes.group.scope_logic import (
    scope_difference,
    scope_union,
)

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


class ProjectCapabilities(UserDict[tuple[type[Acl], AclName, AclAction], Scope]):
    """A helper class to represent the capabilities for a given CDF Project.

    The capabilities are stored as a mapping from (ACL type, AclName, action) to scope, which allows for easy verification of ACLs against the capabilities.
    Note that AclName is included to account for UnknownAcls, which may have the same ACL type but different names, and thus different capabilities.
    """

    def __init__(
        self, capabilities: dict[tuple[type[Acl], AclName, AclAction], Scope], name: str, groups: list[int]
    ) -> None:
        super().__init__(capabilities)
        self.name = name
        self.groups = groups

    def verify(self, acls: Sequence[Acl]) -> Sequence[Acl]:
        """Verify that the provided ACLs are covered by the capabilities in this project.

        Args:
            acls: The ACLs to verify.

        Returns:
            The list of ACLs that are not covered by the capabilities in this project.
        """
        missing_actions_by_type_and_scope: dict[tuple[type[Acl], AclName, ScopeDefinition], list[str]] = defaultdict(
            list
        )
        for acl in acls:
            for action in acl.actions:
                key = (type(acl), acl.acl_name, action)
                if key not in self.data:
                    missing_actions_by_type_and_scope[(type(acl), acl.acl_name, acl.scope)].append(action)
                    continue
                if missing_scope := scope_difference(acl.scope, self.data[key]):
                    missing_actions_by_type_and_scope[(type(acl), acl.acl_name, missing_scope)].append(action)

        missing_acls: list[Acl] = []
        for (acl_type, acl_name, scope), actions in missing_actions_by_type_and_scope.items():
            missing_acls.append(acl_type(actions=actions, acl_name=acl_name, scope=scope))  # type: ignore[arg-type]
        return missing_acls


class InspectResponse(BaseModelObject):
    """Response from the ``GET /api/v1/token/inspect`` endpoint."""

    subject: str
    projects: list[InspectProjectInfo]
    capabilities: list[InspectCapability]
    # This is not part of the API response, but we manually set it to the current project as it is very useful
    project: str = ""

    def to_project_capabilities(self, project: str | None = None) -> ProjectCapabilities:
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
            raise ValueError(f"Project '{project}' not found in inspect response")

        scopes_by_acl_action: dict[tuple[type[Acl], AclName, AclAction], list[Scope]] = defaultdict(list)
        for capability in self.capabilities:
            if not (
                isinstance(capability.project_scope, AllProjects)
                or (isinstance(capability.project_scope, ProjectList) and project in capability.project_scope.projects)
            ):
                continue
            for action in capability.acl.actions:
                scopes_by_acl_action[(type(capability.acl), capability.acl.acl_name, action)].append(
                    capability.acl.scope
                )

        scope_by_acl_action: dict[tuple[type[Acl], AclName, AclAction], Scope] = {}
        for key, scopes in scopes_by_acl_action.items():
            try:
                union = scope_union(*scopes)
            except TypeError:
                if any(isinstance(scope, UnknownScope) for scope in scopes):
                    # We use ProjectCapabilities to verify whether we have a specific set of required capabilities.
                    # We will never check for an unknown ACL and unknown scope, thus we can safely ignore the TypeError due to
                    # an UnknownScope being unhashable.
                    continue
                raise
            scope_by_acl_action[key] = union
        return ProjectCapabilities(
            capabilities=scope_by_acl_action,
            name=project_info.project_url_name,
            groups=project_info.groups,
        )
