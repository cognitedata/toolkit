from typing import Literal

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
)

from ._auth import BasicAuthenticationRequest, BasicAuthenticationResponse
from ._base import SourceRequestDefinition, SourceResponseDefinition
from ._certificate import AuthCertificateRequest, CACertificateRequest, CertificateResponse


class MQTTSource(BaseModelObject):
    type: Literal["mqtt5", "mqtt3"]
    host: str
    port: int | None = None
    use_tls: bool | None = None


class MQTTSourceRequest(MQTTSource, SourceRequestDefinition):
    authentication: BasicAuthenticationRequest | None = None
    ca_certificate: CACertificateRequest | None = None
    auth_certificate: AuthCertificateRequest | None = None


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
