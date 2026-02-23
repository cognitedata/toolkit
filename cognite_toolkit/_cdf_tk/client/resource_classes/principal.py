"""Principal resource classes for the Cognite Principals API.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Principals
"""

from abc import ABC
from typing import Annotated, Literal, TypeAlias

from pydantic import Field

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject

PrincipalType: TypeAlias = Literal["SERVICE_ACCOUNT", "USER"]


class CreatedBy(BaseModelObject):
    org_id: str
    user_id: str


class PrincipalDefinition(BaseModelObject, ABC):
    id: str
    type: Literal["SERVICE_ACCOUNT", "USER"]
    name: str
    picture_url: str


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


Principal = Annotated[
    ServiceAccountPrincipal | UserPrincipal,
    Field(discriminator="type"),
]


class LoginSession(BaseModelObject):
    id: str
    created_time: int
    status: Literal["ACTIVE", "LOGGED_OUT", "EXPIRED", "REVOKED"]
    deactivated_time: int | None = None
    # This is not part of the API response, but we include it here to track which principal the session belongs to.
    principal: str = Field("")
