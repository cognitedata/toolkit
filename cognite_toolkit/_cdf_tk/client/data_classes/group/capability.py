"""GroupCapability wrapper for Group capabilities.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Groups/operation/createGroups
"""

from typing import Annotated, Any

from pydantic import BeforeValidator, model_serializer, model_validator
from pydantic_core.core_schema import SerializerFunctionWrapHandler

from cognite_toolkit._cdf_tk.client.data_classes.base import BaseModelObject

from .acls import ACL_CLASS_BY_NAME, Acl, UnknownAcl, parse_acl
from .scopes import parse_scope


class ProjectUrlNames(BaseModelObject):
    """Project URL names for cross-project capabilities."""

    url_names: list[str]


class GroupCapability(BaseModelObject):
    """A single capability entry containing an ACL and optional project URL names."""

    acl: Acl
    project_url_names: ProjectUrlNames | None = None

    @model_validator(mode="before")
    @classmethod
    def parse_capability(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data

        result: dict[str, Any] = {}
        acl_found = False

        for key, value in data.items():
            if key == "projectUrlNames":
                result["project_url_names"] = ProjectUrlNames.model_validate(value)
            elif key in ACL_CLASS_BY_NAME:
                result["acl"] = parse_acl(key, value)
                acl_found = True
            elif not acl_found and isinstance(value, dict) and "actions" in value:
                # Unknown ACL type
                result["acl"] = UnknownAcl(
                    actions=value.get("actions", []),
                    scope=parse_scope(value.get("scope", {"all": {}})),
                )
                acl_found = True

        if not acl_found:
            raise ValueError(f"No ACL found in capability data: {data}")

        return result

    @model_serializer(mode="wrap", when_used="always", return_type=dict)
    def serialize_capability(self, handler: SerializerFunctionWrapHandler) -> dict:
        result = self.acl.dump(camel_case=True)
        if self.project_url_names is not None:
            result["projectUrlNames"] = self.project_url_names.dump(camel_case=True)
        return result


def _handle_capability(value: Any) -> Any:
    """Validator to handle capability parsing."""
    if isinstance(value, GroupCapability):
        return value
    if isinstance(value, dict):
        return GroupCapability.model_validate(value)
    return value


GroupCapabilityType = Annotated[GroupCapability, BeforeValidator(_handle_capability)]
