import builtins
from typing import ClassVar, Literal

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
)

from ._auth import (
    AuthenticationRequestUnion,
    AuthenticationResponseUnion,
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
    non_nullable_fields: ClassVar[frozenset[str]] = frozenset({"use_tls"})
    authentication: AuthenticationRequestUnion | None = None
    ca_certificate: CACertificateRequest | None = None
    auth_certificate: AuthCertificateRequest | None = None


class KafkaSourceResponse(
    SourceResponseDefinition,
    KafkaSource,
    ResponseResource[KafkaSourceRequest],
):
    authentication: AuthenticationResponseUnion | None = None
    ca_certificate: CertificateResponse | None = None
    auth_certificate: CertificateResponse | None = None

    @classmethod
    def request_cls(cls) -> builtins.type[KafkaSourceRequest]:
        return KafkaSourceRequest
