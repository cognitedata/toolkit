from typing import ClassVar, Literal

from pydantic import Field, SecretStr

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


class Authentication(BaseModelResource):
    type: ClassVar[str]


class BasicAuthentication(Authentication):
    type: ClassVar[str] = "basic"
    username: str = Field(
        description="Username used for basic authentication.",
        max_length=200,
    )
    password: SecretStr = Field(
        description="Password used for basic authentication.",
        max_length=200,
    )


class ClientCredentials(Authentication):
    type: ClassVar[str] = "clientCredentials"
    client_id: str = Field(
        description="Client ID for for the service principal used by the extractor",
    )
    client_secret: SecretStr = Field(description="Client secret for for the service principal used by the extractor")
    token_url: str = Field(
        description="URL to fetch authentication tokens from",
    )
    scope: str = Field(
        description="A space separated list of scopes",
    )
    default_expires_in: str | None = Field(
        None,
        description="Default value for the expires_in OAuth 2.0 parameter. If the identity provider does not return expires_in in token requests, this parameter must be set or the request will fail.",
    )


class QueryCredentials(Authentication):
    key: str = Field(
        description="Key for the query parameter to place the authentication token in.",
    )
    value: SecretStr = Field(description="Value of the authentication token")


class HeaderCredentials(Authentication):
    key: str = Field(
        description="Key for the header to place the authentication token in",
    )
    value: SecretStr = Field(description="Value of the authentication token")


class ScramSha(Authentication):
    username: str = Field(
        description="Username for authentication",
        max_length=200,
    )
    password: SecretStr = Field(
        description="Password for authentication",
        max_length=200,
    )


class ScramSha256(ScramSha):
    type: ClassVar[str] = "scramSha256"


class ScramSha512(ScramSha):
    type: ClassVar[str] = "scramSha512"


class HostedExtractorSourceYAML(ToolkitResource):
    type: ClassVar[str]
    external_id: str = Field(
        description="The external ID provided by the client. Must be unique for the resource type.",
        max_length=255,
    )


class EventHubSource(HostedExtractorSourceYAML):
    type: ClassVar[str] = "eventhub"
    host: str = Field(
        description="Host name or IP address of the event hub consumer endpoint.",
        max_length=200,
    )
    event_hub_name: str = Field(
        description="Name of the event hub",
        max_length=200,
    )
    key_name: str = Field(
        description="The name of the Event Hub key to use for authentication.",
        max_length=200,
    )
    key_value: str = Field(
        description="Value of the Event Hub key to use for authentication.",
        max_length=200,
    )
    consumer_group: str | None = Field(
        None,
        description="The event hub consumer group to use. Microsoft recommends having a distinct consumer group for each application consuming data from event hub. If left out, this uses the default consumer group.",
        max_length=200,
    )


class RESTSource(HostedExtractorSourceYAML):
    type: ClassVar[str] = "rest"
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
    authentication: Authentication | None = Field(None, description="Authentication details for source")


class MQTTSource(HostedExtractorSourceYAML):
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
    type: ClassVar[str] = "mqtt3"


class MQTT5Source(MQTTSource):
    type: ClassVar[str] = "mqtt5"


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


class KafkaSource(HostedExtractorSourceYAML):
    type: ClassVar[str] = "kafka"
    bootstrap_brokers: list[KafkaBroker] = Field(
        description="List of redundant kafka brokers to connect to.", min_length=1, max_length=8
    )
    authentication: Authentication | None = Field(None, description="Authentication details for source")
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
