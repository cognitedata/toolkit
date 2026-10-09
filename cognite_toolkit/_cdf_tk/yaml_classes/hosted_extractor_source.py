from abc import ABC
from typing import Annotated, Literal

from pydantic import Field, SecretStr, field_serializer

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId

from .base import BaseModelResource, ToolkitResource


class CACertificate(BaseModelResource):
    type: Literal["der", "pem"] = Field(
        description="Type of certificate in the certificate field.",
    )
    certificate: str = Field(
        description="Base 64 encoded der certificate, or a pem certificate with headers.",
        max_length=100000,
    )


class AuthCertificate(BaseModelResource):
    key: str = Field(
        description="The key for the certificate",
        max_length=100000,
    )
    key_password: SecretStr | None = Field(
        None,
        description="The password for the certificate key",
        min_length=1,
        max_length=255,
    )
    type: Literal["der", "pem"] = Field(
        description="Type of certificate in the certificate field.",
    )
    certificate: str = Field(
        description="Base 64 encoded der certificate, or a pem certificate with headers.",
        max_length=100000,
    )

    @field_serializer("key_password", when_used="json")
    def dump_key_password(self, v: SecretStr | None) -> str | None:
        return v.get_secret_value() if v else None


class Authentication(BaseModelResource):
    type: str


class BasicAuthentication(Authentication):
    type: Literal["basic"] = Field("basic")
    username: str = Field(
        description="Username used for basic authentication.",
        max_length=200,
    )
    password: SecretStr = Field(
        description="Password used for basic authentication.",
        max_length=200,
    )

    @field_serializer("password", when_used="json")
    def dump_password(self, v: SecretStr) -> str:
        return v.get_secret_value()


class ClientCredentials(Authentication):
    type: Literal["clientCredentials"] = Field("clientCredentials")
    client_id: str = Field(
        description="Client ID for for the service principal used by the extractor",
    )
    client_secret: SecretStr = Field(description="Client secret for for the service principal used by the extractor")
    token_url: str = Field(
        description="URL to fetch authentication tokens from",
    )
    scopes: str | list[str] = Field(
        description="A space separated list of scopes",
    )
    default_expires_in: str | None = Field(
        None,
        description="Default value for the expires_in OAuth 2.0 parameter. If the identity provider does not return expires_in in token requests, this parameter must be set or the request will fail.",
    )

    @field_serializer("client_secret", when_used="json")
    def dump_client_secret(self, v: SecretStr) -> str:
        return v.get_secret_value()


class QueryCredentials(Authentication):
    type: Literal["query"] = Field("query")
    key: str = Field(
        description="Key for the query parameter to place the authentication token in.",
    )
    value: SecretStr = Field(description="Value of the authentication token")

    @field_serializer("value", when_used="json")
    def dump_value(self, v: SecretStr) -> str:
        return v.get_secret_value()


class HeaderCredentials(Authentication):
    type: Literal["header"] = Field("header")
    key: str = Field(
        description="Key for the header to place the authentication token in",
    )
    value: SecretStr = Field(description="Value of the authentication token")

    @field_serializer("value", when_used="json")
    def dump_value(self, v: SecretStr) -> str:
        return v.get_secret_value()


class ScramSha(Authentication, ABC):
    username: str = Field(
        description="Username for authentication",
        max_length=200,
    )
    password: SecretStr = Field(
        description="Password for authentication",
        max_length=200,
    )

    @field_serializer("password", when_used="json")
    def dump_password(self, v: SecretStr) -> str:
        return v.get_secret_value()


class ScramSha256(ScramSha):
    type: Literal["scramSha256"] = Field("scramSha256")


class ScramSha512(ScramSha):
    type: Literal["scramSha512"] = Field("scramSha512")


RESTAuthentication = Annotated[
    BasicAuthentication | ClientCredentials | QueryCredentials | HeaderCredentials,
    Field(discriminator="type"),
]

KafkaAuthentication = Annotated[
    BasicAuthentication | ClientCredentials | ScramSha256 | ScramSha512,
    Field(discriminator="type"),
]


class HostedExtractorSource(ToolkitResource):
    type: str
    external_id: str = Field(
        description="The external ID provided by the client. Must be unique for the resource type.",
        max_length=255,
    )

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class EventHubSource(HostedExtractorSource):
    type: Literal["eventhub"] = Field("eventhub")
    host: str = Field(
        description="Host name or IP address of the event hub consumer endpoint.",
        max_length=200,
    )
    event_hub_name: str = Field(
        description="Name of the event hub",
        max_length=200,
    )
    key_name: str | None = Field(
        None,
        description="The name of the Event Hub key to use for authentication.",
        max_length=200,
    )
    key_value: SecretStr | None = Field(
        None,
        description="Value of the Event Hub key to use for authentication.",
        max_length=200,
    )
    consumer_group: str | None = Field(
        None,
        description="The event hub consumer group to use. Microsoft recommends having a distinct consumer group for each application consuming data from event hub. If left out, this uses the default consumer group.",
        max_length=200,
    )

    @field_serializer("key_value", when_used="json")
    def dump_secret(self, v: SecretStr | None) -> str | None:
        return v.get_secret_value() if v else None


class RESTSource(HostedExtractorSource):
    type: Literal["rest"] = Field("rest")
    host: str = Field(
        description="Host or IP address to connect to.",
        max_length=200,
    )
    scheme: Literal["http", "https"] = Field(
        "https",
        description="Type of connection to establish",
    )
    port: int | None = Field(
        None,
        description="Port on server to connect to. Uses default ports based on the scheme if omitted.",
        ge=1,
        le=65535,
    )
    ca_certificate: str | CACertificate | None = Field(
        None,
        description="Custom certificate authority certificate to let the source use a self signed certificate.",
    )
    authentication: RESTAuthentication | None = Field(None, description="Authentication details for source")


class MQTTSource(HostedExtractorSource, ABC):
    host: str = Field(
        description="Host or IP address of the MQTT broker to connect to.",
        max_length=200,
    )
    port: int | None = Field(
        None,
        description="Port on the MQTT broker to connect to.",
        ge=1,
        le=65535,
    )
    authentication: BasicAuthentication | None = Field(
        None,
        description="Method used for authenticating with the mqtt broker. This may be used together with auth certificate.",
    )
    use_tls: bool | None = Field(
        None,
        description="If true, use TLS when connecting to the broker.",
    )
    ca_certificate: CACertificate | None = Field(
        None,
        description="Custom certificate authority certificate to let the source use a self signed certificate.",
    )
    auth_certificate: AuthCertificate | None = Field(
        None,
        description="Authentication certificate (if configured) used to authenticate to source.",
    )


class MQTT3Source(MQTTSource):
    type: Literal["mqtt3"] = Field("mqtt3")


class MQTT5Source(MQTTSource):
    type: Literal["mqtt5"] = Field("mqtt5")


class KafkaBroker(BaseModelResource):
    host: str = Field(
        description="Host name or IP address of the bootstrap broker.",
        max_length=200,
    )
    port: int = Field(
        description="Port on the bootstrap broker to connect to.",
        ge=1,
        le=65535,
    )


class KafkaSource(HostedExtractorSource):
    type: Literal["kafka"] = Field("kafka")
    bootstrap_brokers: list[KafkaBroker] = Field(
        description="List of redundant kafka brokers to connect to.", min_length=1, max_length=8
    )
    authentication: KafkaAuthentication | None = Field(None, description="Authentication details for source")
    use_tls: bool | None = Field(
        None,
        description="If true, use TLS when connecting to the broker",
    )
    ca_certificate: CACertificate | None = Field(
        None,
        description="Custom certificate authority certificate to let the source use a self signed certificate.",
    )
    auth_certificate: AuthCertificate | None = Field(
        None,
        description="Authentication certificate (if configured) used to authenticate to source.",
    )


class MQTTBroker(HostedExtractorSource):
    type: Literal["mqtt_broker"] = Field("mqtt_broker")
    external_id: str = Field(
        description="The external ID provided by the client. Must be unique for the resource type.",
        max_length=255,
    )
    name: str | None = Field(
        None,
        description="Name of the MQTT broker.",
        max_length=50,
    )
    description: str | None = Field(
        None,
        description="Description of the MQTT broker.",
        max_length=500,
    )
    metadata: dict[str, str] | None = Field(
        None,
        description="Metadata of the MQTT broker.",
        max_length=16,
    )


HostedExtractorSourceYAML = Annotated[
    EventHubSource | RESTSource | MQTT3Source | MQTT5Source | KafkaSource | MQTTBroker,
    Field(discriminator="type"),
]
