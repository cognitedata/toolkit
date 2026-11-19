"""Organization resource classes for the Cognite Organizations API.

Based on the API specification at:
https://api-docs.cognite.com/20230101-alpha/tag/Organizations
"""

from abc import ABC
from typing import Annotated, Any, Literal, TypeAlias

from pydantic import BeforeValidator, Field

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, RequestResource, ResponseResource
from cognite_toolkit._cdf_tk.client.identifiers import NameId, OrgId
from cognite_toolkit._cdf_tk.utils._auxiliary import registry_from_subclasses_with_type_field

ProjectState: TypeAlias = Literal["ACTIVE", "LOCKED", "DELETED"]


class ExtraAuthParameter(BaseModelObject):
    name: str
    value: str


class IdentityProviderDefinition(BaseModelObject, ABC):
    """Configuration for an external OIDC-compliant IdP.

    Secrets (client secret, app key) are write-only and are not returned by the API.
    """

    idp_vendor: str
    issuer: str


class AzureAdIdp(IdentityProviderDefinition):
    idp_vendor: Literal["AZURE_AD"] = "AZURE_AD"


class Auth0Idp(IdentityProviderDefinition):
    idp_vendor: Literal["AUTH0"] = "AUTH0"
    client_id: str
    client_secret: str | None = None
    groups_claim_name: str
    extra_auth_parameters: list[ExtraAuthParameter] | None = None


class KeycloakIdp(IdentityProviderDefinition):
    idp_vendor: Literal["KEYCLOAK"] = "KEYCLOAK"
    client_id: str


class SAuthIdp(IdentityProviderDefinition):
    idp_vendor: Literal["SAUTH"] = "SAUTH"
    client_id: str
    client_secret: str | None = None
    app_key: str | None = None
    groups_url: str
    id_token_audience: str
    extra_scope: str


class AWSCognitoIdp(IdentityProviderDefinition):
    idp_vendor: Literal["AWS_COGNITO"] = "AWS_COGNITO"
    client_id: str
    client_secret: str | None = None


class OktaIdp(IdentityProviderDefinition):
    idp_vendor: Literal["OKTA"] = "OKTA"
    client_id: str
    client_secret: str | None = None
    groups_claim_name: str
    access_token_audience: str


class UnknownIdp(IdentityProviderDefinition):
    idp_vendor: str


def _handle_unknown_idp(value: Any) -> Any:
    if isinstance(value, dict):
        idp_cls = _IDP_BY_VENDOR.get(value.get("idpVendor", value.get("idp_vendor")), UnknownIdp)
        return idp_cls.model_validate(value)
    return value


_IDP_BY_VENDOR = registry_from_subclasses_with_type_field(
    IdentityProviderDefinition,
    type_field="idp_vendor",
    exclude=(UnknownIdp,),
)

IdentityProvider = Annotated[
    AzureAdIdp | Auth0Idp | KeycloakIdp | SAuthIdp | AWSCognitoIdp | OktaIdp | UnknownIdp,
    BeforeValidator(_handle_unknown_idp),
]


class ContactPerson(BaseModelObject):
    id: int
    email: str
    name: str | None = None
    phone: str | None = None
    note: str | None = None


class Org(BaseModelObject):
    id: str
    idp: IdentityProvider
    migration_status: str | None = None
    admin_group_id: str | None = None
    admins_can_create_orgs_in_subtree: bool = False
    admins_can_create_projects_in_subtree: bool = False
    allowed_clusters: list[str] = Field(default_factory=list)

    def as_id(self) -> OrgId:
        return OrgId(id=self.id)


class OrgRequest(Org, RequestResource):
    """Request to create a child organization."""


class OrgResponse(Org, ResponseResource[OrgRequest]):
    parent_id: str
    is_deleted: bool = False
    profiles_enabled: bool = True

    @classmethod
    def request_cls(cls) -> type[OrgRequest]:
        return OrgRequest


class OrgWithContactPersonsResponse(OrgResponse):
    """Response from retrieving a single organization."""

    contact_persons: list[ContactPerson]


class OrgProject(BaseModelObject):
    name: str
    cluster_name: str
    project_admin_group_id: str | None = None

    def as_id(self) -> NameId:
        return NameId(name=self.name)


class OrgProjectRequest(OrgProject, RequestResource):
    """Request to create a project in an organization."""


class OrgProjectCreateResponse(OrgProject, ResponseResource[OrgProjectRequest]):
    """Response from creating a project. The admin group ID may differ from the one requested."""

    @classmethod
    def request_cls(cls) -> type[OrgProjectRequest]:
        return OrgProjectRequest


class OrgProjectResponse(BaseModelObject):
    """A project in an organization. The cluster name, state and deletion time are only
    returned when admin properties are requested."""

    name: str
    api_url: str
    cluster_name: str | None = None
    state: ProjectState | None = None
    deletion_time: int | None = None

    def as_id(self) -> NameId:
        return NameId(name=self.name)
