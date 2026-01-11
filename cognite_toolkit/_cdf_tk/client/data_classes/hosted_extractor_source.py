from typing import Annotated, Literal

from pydantic import Field, TypeAdapter

from cognite_toolkit._cdf_tk.client.data_classes.base import (
    BaseModelObject,
    RequestResource,
    ResponseResource,
)

from .identifiers import ExternalId


class CACertificateRequest(BaseModelObject):
    type: str
    certificate: str


class AuthCertificateRequest(BaseModelObject):
    key: str
    key_password: str | None = None
    type: str
    certificate: str


class CertificateResponse(BaseModelObject):
    thumbprint: str
    expires_at: int


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


AuthenticationRequest = Annotated[
    BasicAuthenticationRequest | ClientCredentialAuthenticationRequest | ScramShaAuthenticationRequest,
    Field(discriminator="type"),
]

AuthenticationResponse = Annotated[
    BasicAuthenticationResponse | ClientCredentialAuthenticationResponse | ScramShaAuthenticationResponse,
    Field(discriminator="type"),
]


class KafkaBroker(BaseModelObject):
    host: str
    port: int


class HostedExtractorSourceRequestDefinition(RequestResource):
    type: str
    external_id: str

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class HostedExtractorKafkaSource(BaseModelObject):
    type: Literal["kafka"] = "kafka"
    bootstrap_brokers: list[KafkaBroker]
    use_tls: bool | None = None


class HostedExtractorKafkaSourceRequest(HostedExtractorKafkaSource, HostedExtractorSourceRequestDefinition):
    authentication: AuthenticationRequest | None = None
    ca_certificate: CACertificateRequest | str | None = None
    auth_certificate: AuthCertificateRequest | None = None


class HostedExtractorEventHubSource(BaseModelObject):
    type: Literal["eventHub"] = "eventHub"
    host: str
    port: int | None = None
    event_hub_name: str
    key_name: str
    consumer_group: str | None = None
    scheme: str | None = None
    use_tls: bool | None = None


class HostedExtractorEventHubSourceRequest(HostedExtractorEventHubSource, HostedExtractorSourceRequestDefinition):
    key_value: str
    ca_certificate: CACertificateRequest | str | None = None
    auth_certificate: AuthCertificateRequest | None = None
    authentication: AuthenticationRequest | None = None


class HostedExtractorMQTTSource(BaseModelObject):
    type: Literal["mqtt"] = "mqtt"
    host: str
    port: int | None = None
    use_tls: bool | None = None


class HostedExtractorMQTTSourceRequest(HostedExtractorMQTTSource, HostedExtractorSourceRequestDefinition):
    authentication: AuthenticationRequest | None = None
    ca_certificate: CACertificateRequest | str | None = None
    auth_certificate: AuthCertificateRequest | None = None


class HostedExtractorRESTSource(BaseModelObject):
    type: Literal["rest"] = "rest"
    host: str
    port: int | None = None
    use_tls: bool | None = None


class HostedExtractorRESTSourceRequest(HostedExtractorRESTSource, HostedExtractorSourceRequestDefinition):
    authentication: AuthenticationRequest | None = None
    ca_certificate: CACertificateRequest | str | None = None
    auth_certificate: AuthCertificateRequest | None = None


class HostedExtractorSourceResponseDefinition(BaseModelObject):
    type: str
    external_id: str
    created_time: int
    last_updated_time: int


class HostedExtractorKafkaSourceResponse(
    HostedExtractorSourceResponseDefinition,
    HostedExtractorKafkaSource,
    ResponseResource[HostedExtractorKafkaSourceRequest],
):
    authentication: AuthenticationResponse | None = None
    ca_certificate: CertificateResponse | None = None
    auth_certificate: CertificateResponse | None = None

    def as_request_resource(self) -> HostedExtractorKafkaSourceRequest:
        return HostedExtractorKafkaSourceRequest.model_validate(self.dump(), extra="ignore")


class HostedExtractorEventHubSourceResponse(
    HostedExtractorSourceResponseDefinition,
    HostedExtractorEventHubSource,
    ResponseResource[HostedExtractorEventHubSourceRequest],
):
    authentication: AuthenticationResponse | None = None
    ca_certificate: CertificateResponse | None = None
    auth_certificate: CertificateResponse | None = None

    def as_request_resource(self) -> HostedExtractorEventHubSourceRequest:
        return HostedExtractorEventHubSourceRequest.model_validate(self.dump(), extra="ignore")


class HostedExtractorMQTTSourceResponse(
    HostedExtractorSourceResponseDefinition,
    HostedExtractorMQTTSource,
    ResponseResource[HostedExtractorMQTTSourceRequest],
):
    authentication: AuthenticationResponse | None = None
    ca_certificate: CertificateResponse | None = None
    auth_certificate: CertificateResponse | None = None

    def as_request_resource(self) -> HostedExtractorMQTTSourceRequest:
        return HostedExtractorMQTTSourceRequest.model_validate(self.dump(), extra="ignore")


class HostedExtractorRESTSourceResponse(
    HostedExtractorSourceResponseDefinition,
    HostedExtractorRESTSource,
    ResponseResource[HostedExtractorRESTSourceRequest],
):
    authentication: AuthenticationResponse | None = None
    ca_certificate: CertificateResponse | None = None
    auth_certificate: CertificateResponse | None = None

    def as_request_resource(self) -> HostedExtractorRESTSourceRequest:
        return HostedExtractorRESTSourceRequest.model_validate(self.dump(), extra="ignore")


HostedExtractorSourceRequestUnion = Annotated[
    HostedExtractorKafkaSourceRequest
    | HostedExtractorEventHubSourceRequest
    | HostedExtractorMQTTSourceRequest
    | HostedExtractorRESTSourceRequest,
    Field(discriminator="type"),
]

HostedExtractorSourceRequest: TypeAdapter[HostedExtractorSourceRequestUnion] = TypeAdapter(
    HostedExtractorSourceRequestUnion
)

HostedExtractorSourceResponseUnion = Annotated[
    HostedExtractorKafkaSourceResponse
    | HostedExtractorEventHubSourceResponse
    | HostedExtractorMQTTSourceResponse
    | HostedExtractorRESTSourceResponse,
    Field(discriminator="type"),
]

HostedExtractorSourceResponse: TypeAdapter[HostedExtractorSourceResponseUnion] = TypeAdapter(
    HostedExtractorSourceResponseUnion
)
