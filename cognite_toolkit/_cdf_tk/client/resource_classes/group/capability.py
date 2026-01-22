"""GroupCapability wrapper for Group capabilities.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Groups/operation/createGroups
"""

from typing import Any

from pydantic import model_serializer, model_validator
from pydantic_core.core_schema import FieldSerializationInfo

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.client.resource_classes.group._constants import ACL_NAME

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
        value_copy = value.copy()
        acl_data = dict(value_copy.pop(acl_name))
        acl_data[ACL_NAME] = acl_name
        value_copy["acl"] = acl_data
        return value_copy

    # MyPy complains that info; FieldSerializationInfo is not compatible with info: Any
    # It is.
    @model_serializer  # type: ignore[type-var]
    def serialize_acl_name(self, info: FieldSerializationInfo) -> dict[str, Any]:
        """Serialize 'acl' field back to its specific ACL key (e.g., 'assetsAcl') for API compatibility."""
        acl_data = self.acl.model_dump(**vars(info))
        output: dict[str, Any] = {self.acl.acl_name: acl_data}
        if self.project_url_names is not None:
            output["projectUrlNames"] = self.project_url_names.model_dump(**vars(info))
        return output
