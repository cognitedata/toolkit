from cognite_toolkit._cdf_tk.client.data_classes.base import (
    BaseModelObject,
    RequestResource,
    ResponseResource,
)

from .identifiers import ExternalId


class CACertificate(BaseModelObject):
    type: str | None = None
    certificate: str | None = None


class AuthCertificate(BaseModelObject):
    key: str | None = None
    key_password: str | None = None
    type: str | None = None
    certificate: str | None = None


class Authentication(BaseModelObject):
    type: str | None = None
    username: str | None = None
    password: str | None = None
    client_id: str | None = None
    client_secret: str | None = None
    token_url: str | None = None
    scope: str | None = None
    key: str | None = None
    value: str | None = None


class KafkaBroker(BaseModelObject):
    host: str
    port: int


class HostedExtractorSource(BaseModelObject):
    external_id: str
    type: str | None = None
    host: str | None = None
    port: int | None = None
    event_hub_name: str | None = None
    key_name: str | None = None
    key_value: str | None = None
    consumer_group: str | None = None
    scheme: str | None = None
    ca_certificate: CACertificate | str | None = None
    auth_certificate: AuthCertificate | None = None
    authentication: Authentication | None = None
    use_tls: bool | None = None
    bootstrap_brokers: list[KafkaBroker] | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class HostedExtractorSourceRequest(HostedExtractorSource, RequestResource): ...


class HostedExtractorSourceResponse(HostedExtractorSource, ResponseResource[HostedExtractorSourceRequest]):
    created_time: int
    last_updated_time: int

    def as_request_resource(self) -> HostedExtractorSourceRequest:
        return HostedExtractorSourceRequest.model_validate(self.dump(), extra="ignore")
