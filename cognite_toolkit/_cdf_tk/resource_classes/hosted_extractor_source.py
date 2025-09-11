import sys
from abc import ABC
from types import MappingProxyType
from typing import Any, ClassVar, Literal, cast

from pydantic import (
    Field,
    ModelWrapValidatorHandler,
    SecretStr,
    field_serializer,
    field_validator,
    model_serializer,
    model_validator,
)
from pydantic_core.core_schema import SerializationInfo, SerializerFunctionWrapHandler

from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses

from .base import BaseModelResource, ToolkitResource

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


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
    type: ClassVar[str]

    @model_validator(mode="wrap")
    @classmethod
    def find_source_type(
        cls, data: "dict[str, Any] | Authentication", handler: ModelWrapValidatorHandler[Self]
    ) -> Self:
        if isinstance(data, Authentication):
            return cast(Self, data)
        if not isinstance(data, dict):
            raise ValueError(f"Invalid authentication data '{type(data)}' expected dict")

        if cls is not Authentication:
            # We are already in a subclass, so just validate as usual
            return handler(data)
        # If not we need to find the right subclass based on the type field.
        if "type" not in data:
            raise ValueError("Invalid authentication data missing 'type' key")
        type_ = data["type"]
        if type_ not in _AUTHENTICATION_CLS_BY_TYPE:
            raise ValueError(
                f"invalid authentication type '{type_}'. Expected one of {humanize_collection(_AUTHENTICATION_CLS_BY_TYPE.keys(), bind_word='or')}"
            )
        cls_ = _AUTHENTICATION_CLS_BY_TYPE[type_]
        return cast(Self, cls_.model_validate({k: v for k, v in data.items() if k != "type"}))

    @model_serializer(mode="wrap", when_used="always", return_type=dict)
    def include_type(self, handler: SerializerFunctionWrapHandler) -> dict:
        if self.type is None:
            raise ValueError("Type is not set")
        serialized_data = handler(self)
        serialized_data["type"] = self.type
        return serialized_data


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

    @field_serializer("password", when_used="json")
    def dump_password(self, v: SecretStr) -> str:
        return v.get_secret_value()


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

    @field_serializer("client_secret", when_used="json")
    def dump_client_secret(self, v: SecretStr) -> str:
        return v.get_secret_value()


class QueryCredentials(Authentication):
    type: ClassVar[str] = "query"
    key: str = Field(
        description="Key for the query parameter to place the authentication token in.",
    )
    value: SecretStr = Field(description="Value of the authentication token")

    @field_serializer("value", when_used="json")
    def dump_value(self, v: SecretStr) -> str:
        return v.get_secret_value()


class HeaderCredentials(Authentication):
    type: ClassVar[str] = "header"
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
    type: ClassVar[str] = "scramSha256"


class ScramSha512(ScramSha):
    type: ClassVar[str] = "scramSha512"


class HostedExtractorSourceYAML(ToolkitResource):
    type: ClassVar[str]
    external_id: str = Field(
        description="The external ID provided by the client. Must be unique for the resource type.",
        max_length=255,
    )

    @model_validator(mode="wrap")
    @classmethod
    def find_source_type(
        cls, data: "dict[str, Any] | HostedExtractorSourceYAML", handler: ModelWrapValidatorHandler[Self]
    ) -> Self:
        if isinstance(data, HostedExtractorSourceYAML):
            return cast(Self, data)
        if not isinstance(data, dict):
            raise ValueError(f"Invalid hosted extractor source data '{type(data)}' expected dict")

        if cls is not HostedExtractorSourceYAML:
            # We are already in a subclass, so just validate as usual
            return handler(data)
        # If not we need to find the right subclass based on the type field.
        if "type" not in data:
            raise ValueError("Invalid hosted extractor source data missing 'type' key")
        type_ = data["type"]
        if type_ not in _SOURCE_CLS_BY_TYPE:
            raise ValueError(
                f"Invalid hosted extractor source type='{type_}'. Expected one of {humanize_collection(_SOURCE_CLS_BY_TYPE.keys(), bind_word='or')}"
            )
        cls_ = _SOURCE_CLS_BY_TYPE[type_]
        return cast(Self, cls_.model_validate({k: v for k, v in data.items() if k != "type"}))

    @model_serializer(mode="wrap", when_used="always", return_type=dict)
    def include_type(self, handler: SerializerFunctionWrapHandler) -> dict:
        if self.type is None:
            raise ValueError("Type is not set")
        serialized_data = handler(self)
        serialized_data["type"] = self.type
        return serialized_data


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
    key_value: SecretStr = Field(
        description="Value of the Event Hub key to use for authentication.",
        max_length=200,
    )
    consumer_group: str | None = Field(
        None,
        description="The event hub consumer group to use. Microsoft recommends having a distinct consumer group for each application consuming data from event hub. If left out, this uses the default consumer group.",
        max_length=200,
    )

    @field_serializer("key_value", when_used="json")
    def dump_secret(self, v: SecretStr) -> str:
        return v.get_secret_value()


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

    @field_validator("authentication", mode="after")
    def validate_authentication(cls, value: Authentication | None) -> Authentication | None:
        valid_auth = (BasicAuthentication, ClientCredentials, QueryCredentials, HeaderCredentials)
        if value is not None and not isinstance(value, valid_auth):
            raise ValueError(
                f"Invalid authentication type '{value.type}' for REST source. Expected one of {humanize_collection([a.type for a in valid_auth], bind_word='or')}"
            )
        return value

    @model_serializer(mode="wrap")
    def serialize_authentication(self, handler: SerializerFunctionWrapHandler, info: SerializationInfo) -> dict:
        # Authentication are serialized as dict {}
        # This issue arises because Pydantic's serialization mechanism doesn't automatically
        # handle polymorphic serialization for subclasses of Authentication.
        # To address this, we include the below to explicitly calling model dump on the authentication
        serialized_data = handler(self)
        if self.authentication:
            serialized_data["authentication"] = self.authentication.model_dump(**vars(info))
        return serialized_data


class MQTTSource(HostedExtractorSourceYAML, ABC):
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

    @field_validator("authentication", mode="after")
    def validate_authentication(cls, value: Authentication | None) -> Authentication | None:
        valid_auth = (BasicAuthentication, ClientCredentials, ScramSha256, ScramSha512)
        if value is not None and not isinstance(value, valid_auth):
            raise ValueError(
                f"Invalid authentication type '{value.type}' for Kafka source. Expected one of {humanize_collection([a.type for a in valid_auth], bind_word='or')}"
            )
        return value

    @model_serializer(mode="wrap")
    def serialize_authentication(self, handler: SerializerFunctionWrapHandler, info: SerializationInfo) -> dict:
        # Authentication are serialized as dict {}
        # This issue arises because Pydantic's serialization mechanism doesn't automatically
        # handle polymorphic serialization for subclasses of Authentication.
        # To address this, we include the below to explicitly calling model dump on the authentication
        serialized_data = handler(self)
        if self.authentication:
            serialized_data["authentication"] = self.authentication.model_dump(**vars(info))
        return serialized_data


_SOURCE_CLS_BY_TYPE: MappingProxyType[str, type[HostedExtractorSourceYAML]] = MappingProxyType(
    {source.type: source for source in get_concrete_subclasses(HostedExtractorSourceYAML)}
)

_AUTHENTICATION_CLS_BY_TYPE: MappingProxyType[str, type[Authentication]] = MappingProxyType(
    {auth.type: auth for auth in get_concrete_subclasses(Authentication)}
)
