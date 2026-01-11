from typing import Annotated, Any, Literal

from pydantic import Field, TypeAdapter

from cognite_toolkit._cdf_tk.client.data_classes.base import (
    BaseModelObject,
    RequestUpdateable,
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


class BasicAuthentication(AuthenticationRequestDefinition):
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


class KafkaBroker(BaseModelObject):
    host: str
    port: int


class SourceRequestDefinition(RequestUpdateable):
    type: str
    external_id: str

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)

    def as_update(self, mode: Literal["patch", "replace"]) -> dict[str, Any]:
        output = super().as_update(mode)
        output["type"] = self.type
        return output


class KafkaSource(BaseModelObject):
    type: Literal["kafka"] = "kafka"
    bootstrap_brokers: list[KafkaBroker]
    use_tls: bool | None = None


class KafkaSourceRequest(KafkaSource, SourceRequestDefinition):
    authentication: (
        BasicAuthenticationRequest | ClientCredentialAuthenticationRequest | ScramShaAuthenticationRequest | None
    ) = Field(None, discriminator="type")
    ca_certificate: CACertificateRequest | None = None
    auth_certificate: AuthCertificateRequest | None = None


class EventHubSource(BaseModelObject):
    type: Literal["eventHub"] = "eventHub"
    host: str
    event_hub_name: str
    consumer_group: str | None = None


class EventHubSourceRequest(EventHubSource, SourceRequestDefinition):
    authentication: BasicAuthenticationRequest | None = None


class MQTTSource(BaseModelObject):
    type: Literal["mqtt5", "mqtt3"]
    host: str
    port: int | None = None
    use_tls: bool | None = None


class MQTTSourceRequest(MQTTSource, SourceRequestDefinition):
    authentication: BasicAuthenticationRequest | None = None
    ca_certificate: CACertificateRequest | None = None
    auth_certificate: AuthCertificateRequest | None = None


class RESTSource(BaseModelObject):
    type: Literal["rest"] = "rest"
    host: str
    port: int | None = None


class RESTSourceRequest(RESTSource, SourceRequestDefinition):
    scheme: Literal["https", "http"] | None = None
    authentication: (
        BasicAuthenticationRequest | HTTPBasicAuthenticationRequest | ClientCredentialAuthenticationRequest | None
    ) = Field(None, discriminator="type")
    ca_certificate: CACertificateRequest | str | None = None
    auth_certificate: AuthCertificateRequest | None = None


class SourceResponseDefinition(BaseModelObject):
    external_id: str
    created_time: int
    last_updated_time: int


class KafkaSourceResponse(
    SourceResponseDefinition,
    KafkaSource,
    ResponseResource[KafkaSourceRequest],
):
    authentication: (
        BasicAuthenticationResponse | ClientCredentialAuthenticationResponse | ScramShaAuthenticationResponse | None
    ) = Field(None, discriminator="type")
    ca_certificate: CertificateResponse | None = None
    auth_certificate: CertificateResponse | None = None

    def as_request_resource(self) -> KafkaSourceRequest:
        return KafkaSourceRequest.model_validate(self.dump(), extra="ignore")


class EventHubSourceResponse(
    SourceResponseDefinition,
    EventHubSource,
    ResponseResource[EventHubSourceRequest],
):
    authentication: BasicAuthenticationResponse | None = None

    def as_request_resource(self) -> EventHubSourceRequest:
        return EventHubSourceRequest.model_validate(self.dump(), extra="ignore")


class MQTTSourceResponse(
    SourceResponseDefinition,
    MQTTSource,
    ResponseResource[MQTTSourceRequest],
):
    authentication: BasicAuthenticationResponse | None = None
    ca_certificate: CertificateResponse | None = None
    auth_certificate: CertificateResponse | None = None

    def as_request_resource(self) -> MQTTSourceRequest:
        return MQTTSourceRequest.model_validate(self.dump(), extra="ignore")


class RESTSourceResponse(
    SourceResponseDefinition,
    RESTSource,
    ResponseResource[RESTSourceRequest],
):
    authentication: (
        BasicAuthenticationResponse | HTTPBasicAuthenticationResponse | ClientCredentialAuthenticationResponse | None
    ) = Field(None, discriminator="type")
    ca_certificate: CertificateResponse | None = None
    auth_certificate: CertificateResponse | None = None

    def as_request_resource(self) -> RESTSourceRequest:
        return RESTSourceRequest.model_validate(self.dump(), extra="ignore")


HostedExtractorSourceRequestUnion = Annotated[
    KafkaSourceRequest | EventHubSourceRequest | MQTTSourceRequest | RESTSourceRequest,
    Field(discriminator="type"),
]

HostedExtractorSourceRequest: TypeAdapter[HostedExtractorSourceRequestUnion] = TypeAdapter(
    HostedExtractorSourceRequestUnion
)

HostedExtractorSourceResponseUnion = Annotated[
    KafkaSourceResponse | EventHubSourceResponse | MQTTSourceResponse | RESTSourceResponse,
    Field(discriminator="type"),
]

HostedExtractorSourceResponse: TypeAdapter[HostedExtractorSourceResponseUnion] = TypeAdapter(
    HostedExtractorSourceResponseUnion
)
