"""Principal resource classes for the Cognite Principals API.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Principals
"""

from abc import ABC
from typing import Annotated, Any, Literal, TypeAlias

from pydantic import BeforeValidator, Field

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.client.identifiers import PrincipalId
from cognite_toolkit._cdf_tk.utils._auxiliary import registry_from_subclasses_with_type_field

PrincipalType: TypeAlias = Literal["SERVICE_ACCOUNT", "USER"]


class CreatedBy(BaseModelObject):
    org_id: str
    user_id: str


class PrincipalDefinition(BaseModelObject, ABC):
    id: str
    type: str
    name: str
    picture_url: str

    def as_id(self) -> PrincipalId:
        """Convert the principal definition to a PrincipalId."""
        return PrincipalId(id=self.id)


class ServiceAccountPrincipal(PrincipalDefinition):
    type: Literal["SERVICE_ACCOUNT"] = "SERVICE_ACCOUNT"
    description: str | None = None
    external_id: str | None = None
    created_by: CreatedBy
    created_time: int
    last_updated_time: int


class UserPrincipal(PrincipalDefinition):
    type: Literal["USER"] = "USER"
    email: str | None = None
    given_name: str | None = None
    middle_name: str | None = None
    family_name: str | None = None


class UnknownPrincipal(PrincipalDefinition):
    type: str


def _handle_unknown_principal(value: Any) -> Any:
    if isinstance(value, dict):
        principal_type = value.get("type")
        if principal_type not in _PRINCIPAL_BY_TYPE:
            return UnknownPrincipal.model_validate(value)
        return _PRINCIPAL_BY_TYPE[principal_type].model_validate(value)
    return value


_PRINCIPAL_BY_TYPE = registry_from_subclasses_with_type_field(
    PrincipalDefinition,
    type_field="type",
    exclude=(UnknownPrincipal,),
)


Principal = Annotated[
    ServiceAccountPrincipal | UserPrincipal | UnknownPrincipal,
    BeforeValidator(_handle_unknown_principal),
]


class LoginSession(BaseModelObject):
    id: str
    created_time: int
    status: Literal["ACTIVE", "LOGGED_OUT", "EXPIRED", "REVOKED"]
    deactivated_time: int | None = None
    # This is not part of the API response, but we include it here to track which principal the session belongs to.
    principal: str = Field("")
