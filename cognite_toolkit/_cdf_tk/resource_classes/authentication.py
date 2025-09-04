from pydantic import Field, SecretStr

from .base import BaseModelResource


class AuthenticationClientIdSecret(BaseModelResource):
    client_id: str = Field(description="Client Id.")
    client_secret: SecretStr = Field(description="Client Secret.")


class OIDCCredential(AuthenticationClientIdSecret):
    scopes: str | list[str] | None = Field(default=None, description="Scopes for the authentication.")
    token_uri: str = Field(description="OAuth token url.")
    cdf_project_name: str = Field(description="CDF project name.")
    audience: str | None = Field(default=None, description="Audience for the authentication.")
