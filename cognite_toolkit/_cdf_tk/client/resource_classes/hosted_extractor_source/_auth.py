from typing import Annotated, Any, Literal

from pydantic import BeforeValidator, ConfigDict

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
)
from cognite_toolkit._cdf_tk.utils._auxiliary import registry_from_subclasses_with_type_field


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
    type: Literal["clientCredentials"] = "clientCredentials"
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


class UnknownAuthenticationRequest(AuthenticationRequestDefinition):
    model_config = ConfigDict(extra="allow")
    type: str


class UnknownAuthenticationResponse(AuthenticationResponseDefinition):
    model_config = ConfigDict(extra="allow")
    type: str


def _handle_authentication_request(value: Any) -> Any:
    if isinstance(value, dict):
        auth_type = value.get("type")
        if auth_type not in _AUTHENTICATION_REQUEST_BY_TYPE:
            return UnknownAuthenticationRequest.model_validate(value)
        return _AUTHENTICATION_REQUEST_BY_TYPE[auth_type].model_validate(value)
    return value


def _handle_authentication_response(value: Any) -> Any:
    if isinstance(value, dict):
        auth_type = value.get("type")
        if auth_type not in _AUTHENTICATION_RESPONSE_BY_TYPE:
            return UnknownAuthenticationResponse.model_validate(value)
        return _AUTHENTICATION_RESPONSE_BY_TYPE[auth_type].model_validate(value)
    return value


_AUTHENTICATION_REQUEST_BY_TYPE = registry_from_subclasses_with_type_field(
    AuthenticationRequestDefinition,
    type_field="type",
    exclude=(UnknownAuthenticationRequest,),
)
_AUTHENTICATION_RESPONSE_BY_TYPE = registry_from_subclasses_with_type_field(
    AuthenticationResponseDefinition,
    type_field="type",
    exclude=(UnknownAuthenticationResponse,),
)


AuthenticationRequestUnion = Annotated[
    BasicAuthenticationRequest
    | ClientCredentialAuthenticationRequest
    | ScramShaAuthenticationRequest
    | HTTPBasicAuthenticationRequest
    | UnknownAuthenticationRequest,
    BeforeValidator(_handle_authentication_request),
]

AuthenticationResponseUnion = Annotated[
    BasicAuthenticationResponse
    | ClientCredentialAuthenticationResponse
    | ScramShaAuthenticationResponse
    | HTTPBasicAuthenticationResponse
    | UnknownAuthenticationResponse,
    BeforeValidator(_handle_authentication_response),
]
