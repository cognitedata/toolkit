from typing import Literal

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
)


class AuthenticationRequestDefinition(BaseModelObject):
    type: str


class AuthenticationResponseDefinition(BaseModelObject):
    type: str


class BasicAuthentication(BaseModelObject):
    type: Literal["basic"] = "basic"
    username: str


class BasicAuthenticationRequest(BasicAuthentication, AuthenticationRequestDefinition):
    password: str | None = None


class BasicAuthenticationResponse(BasicAuthentication, AuthenticationResponseDefinition): ...


class ClientCredentialAuthentication(BaseModelObject):
    type: Literal["clientCredential"] = "clientCredential"
    client_id: str
    token_url: str
    scopes: str
    default_expires_in: str | None = None


class ClientCredentialAuthenticationRequest(ClientCredentialAuthentication, AuthenticationRequestDefinition):
    client_secret: str


class ClientCredentialAuthenticationResponse(ClientCredentialAuthentication, AuthenticationResponseDefinition): ...


class ScramShaAuthentication(BaseModelObject):
    type: Literal["scramSha256", "scramSha512"]
    username: str


class ScramShaAuthenticationRequest(ScramShaAuthentication, AuthenticationRequestDefinition):
    password: str


class ScramShaAuthenticationResponse(ScramShaAuthentication, AuthenticationResponseDefinition): ...


class HTTPBasicAuthenticationRequest(AuthenticationRequestDefinition):
    type: Literal["header", "query"]
    key: str
    value: str


class HTTPBasicAuthenticationResponse(AuthenticationResponseDefinition):
    type: Literal["header", "query"]
    key: str
