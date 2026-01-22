from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
)


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
