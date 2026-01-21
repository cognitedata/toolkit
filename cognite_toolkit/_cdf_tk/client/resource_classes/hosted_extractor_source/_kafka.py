from typing import Literal

from pydantic import Field

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
)

from ._auth import (
    BasicAuthenticationRequest,
    BasicAuthenticationResponse,
    ClientCredentialAuthenticationRequest,
    ClientCredentialAuthenticationResponse,
    ScramShaAuthenticationRequest,
    ScramShaAuthenticationResponse,
)
from ._base import SourceRequestDefinition, SourceResponseDefinition
from ._certificate import AuthCertificateRequest, CACertificateRequest, CertificateResponse


class KafkaBroker(BaseModelObject):
    host: str
    port: int


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
