"""GroupCapability wrapper for Group capabilities.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Groups/operation/createGroups
"""

from typing import Any

from pydantic import model_validator

from cognite_toolkit._cdf_tk.client.data_classes.base import BaseModelObject

from .acls import AclType


class ProjectUrlNames(BaseModelObject):
    """Project URL names for cross-project capabilities."""

    url_names: list[str]


class GroupCapability(BaseModelObject):
    """A single capability entry containing an ACL and optional project URL names."""

    acl: AclType
    project_url_names: ProjectUrlNames | None = None

    @model_validator(mode="before")
    @classmethod
    def move_acl_name(cls, value: Any) -> Any:
        """Move ACL key (e.g., 'assetsAcl') to 'acl' field for API compatibility."""
        if not isinstance(value, dict):
            return value
        if "acl" in value:
            return value
        acl_name = next((key for key in value if key.endswith("Acl")), None)
        if acl_name is None:
            return value
        acl_data = dict(value.pop(acl_name))
        acl_data["aclName"] = acl_name
        value["acl"] = acl_data
        return value
