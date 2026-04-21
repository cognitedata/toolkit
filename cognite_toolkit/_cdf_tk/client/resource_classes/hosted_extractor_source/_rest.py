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


class RESTSource(BaseModelObject):
    type: Literal["rest"] = "rest"
    host: str
    port: int | None = None


class RESTSourceRequest(RESTSource, SourceRequestDefinition):
    non_nullable_fields: ClassVar[frozenset[str]] = frozenset({"scheme", "port"})
    scheme: Literal["https", "http"] | None = None
    authentication: AuthenticationRequestUnion | None = None
    ca_certificate: CACertificateRequest | None = None
    auth_certificate: AuthCertificateRequest | None = None


class RESTSourceResponse(
    SourceResponseDefinition,
    RESTSource,
    ResponseResource[RESTSourceRequest],
):
    authentication: AuthenticationResponseUnion | None = None
    ca_certificate: CertificateResponse | None = None
    auth_certificate: CertificateResponse | None = None

    @classmethod
    def request_cls(cls) -> builtins.type[RESTSourceRequest]:
        return RESTSourceRequest
