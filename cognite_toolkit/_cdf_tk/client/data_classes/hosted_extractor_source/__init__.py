from typing import Annotated

from pydantic import Field, TypeAdapter

from ._auth import (
    BasicAuthenticationRequest,
    BasicAuthenticationResponse,
    ClientCredentialAuthenticationRequest,
    ClientCredentialAuthenticationResponse,
    HTTPBasicAuthenticationRequest,
    HTTPBasicAuthenticationResponse,
    ScramShaAuthenticationRequest,
    ScramShaAuthenticationResponse,
)
from ._certificate import AuthCertificateRequest, CACertificateRequest, CertificateResponse
from ._eventhub import EventHubSourceRequest, EventHubSourceResponse
from ._kafka import KafkaBroker, KafkaSourceRequest, KafkaSourceResponse
from ._mqtt import MQTTSourceRequest, MQTTSourceResponse
from ._rest import RESTSourceRequest, RESTSourceResponse

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


__all__ = [
    "AuthCertificateRequest",
    "BasicAuthenticationRequest",
    "BasicAuthenticationResponse",
    "CACertificateRequest",
    "CertificateResponse",
    "ClientCredentialAuthenticationRequest",
    "ClientCredentialAuthenticationResponse",
    "EventHubSourceRequest",
    "EventHubSourceResponse",
    "HTTPBasicAuthenticationRequest",
    "HTTPBasicAuthenticationResponse",
    "HostedExtractorSourceRequest",
    "HostedExtractorSourceResponse",
    "KafkaBroker",
    "KafkaSourceRequest",
    "KafkaSourceResponse",
    "MQTTSourceRequest",
    "MQTTSourceResponse",
    "RESTSourceRequest",
    "RESTSourceResponse",
    "ScramShaAuthenticationRequest",
    "ScramShaAuthenticationResponse",
]
