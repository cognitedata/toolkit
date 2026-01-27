from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject


class UserProfilesConfiguration(BaseModelObject):
    enabled: bool


class Claim(BaseModelObject):
    claim_name: str


class OidcConfiguration(BaseModelObject):
    jwks_url: str
    token_url: str | None = None
    issuer: str
    audience: str
    skew_ms: int | None = None
    access_claims: list[Claim]
    scope_claims: list[Claim]
    log_claims: list[Claim]
    is_group_callback_enabled: bool | None = None
    identity_provider_scope: str | None = None


class OrganizationResponse(BaseModelObject):
    name: str
    url_name: str
    organization: str
    user_profiles_configuration: UserProfilesConfiguration
    oidc_configuration: OidcConfiguration | None = None
