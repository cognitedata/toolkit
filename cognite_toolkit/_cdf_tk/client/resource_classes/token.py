"""Token inspect response models.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Token/operation/inspectToken
"""

from typing import Any

from pydantic import JsonValue, model_validator

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.client.resource_classes.group._constants import ACL_NAME
from cognite_toolkit._cdf_tk.client.resource_classes.group.acls import AclType


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


class InspectResponse(BaseModelObject):
    """Response from the ``GET /api/v1/token/inspect`` endpoint."""

    subject: str
    projects: list[InspectProjectInfo]
    capabilities: list[InspectCapability]
