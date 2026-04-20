from typing import Annotated, Any

from pydantic import BeforeValidator, TypeAdapter

from cognite_toolkit._cdf_tk.utils._auxiliary import dict_discriminator_value, registry_from_subclasses_with_type_field

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
from ._base import SourceRequestDefinition, SourceResponseDefinition, UnknownSourceRequest, UnknownSourceResponse
from ._certificate import AuthCertificateRequest, CACertificateRequest, CertificateResponse
from ._eventhub import EventHubSourceRequest, EventHubSourceResponse
from ._kafka import KafkaBroker, KafkaSourceRequest, KafkaSourceResponse
from ._mqtt import MQTTSourceRequest, MQTTSourceResponse
from ._rest import RESTSourceRequest, RESTSourceResponse


def _handle_source_request_union(value: Any) -> Any:
    if isinstance(value, dict):
        source_type = dict_discriminator_value(value, "type")
        if source_type not in _SOURCE_REQUEST_BY_TYPE:
            return UnknownSourceRequest.model_validate(value)
        return _SOURCE_REQUEST_BY_TYPE[source_type].model_validate(value)
    return value


def _handle_source_response_union(value: Any) -> Any:
    if isinstance(value, dict):
        source_type = dict_discriminator_value(value, "type")
        if source_type not in _SOURCE_RESPONSE_BY_TYPE:
            return UnknownSourceResponse.model_validate(value)
        return _SOURCE_RESPONSE_BY_TYPE[source_type].model_validate(value)
    return value


_SOURCE_REQUEST_BY_TYPE = registry_from_subclasses_with_type_field(
    SourceRequestDefinition,
    type_field="type",
    exclude=(UnknownSourceRequest,),
)
_SOURCE_RESPONSE_BY_TYPE = registry_from_subclasses_with_type_field(
    SourceResponseDefinition,
    type_field="type",
    exclude=(UnknownSourceResponse,),
)


HostedExtractorSourceRequestUnion = Annotated[
    KafkaSourceRequest | EventHubSourceRequest | MQTTSourceRequest | RESTSourceRequest | UnknownSourceRequest,
    BeforeValidator(_handle_source_request_union),
]

HostedExtractorSourceRequest: TypeAdapter[HostedExtractorSourceRequestUnion] = TypeAdapter(
    HostedExtractorSourceRequestUnion
)

HostedExtractorSourceResponseUnion = Annotated[
    KafkaSourceResponse | EventHubSourceResponse | MQTTSourceResponse | RESTSourceResponse | UnknownSourceResponse,
    BeforeValidator(_handle_source_response_union),
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
    "UnknownSourceRequest",
    "UnknownSourceResponse",
]
