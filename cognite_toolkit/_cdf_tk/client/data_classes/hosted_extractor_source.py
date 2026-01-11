from cognite_toolkit._cdf_tk.client.data_classes.base import (
    BaseModelObject,
    RequestResource,
    ResponseResource,
)

from .identifiers import ExternalId


class HostedExtractorSource(BaseModelObject):
    external_id: str
    type: str | None = None
    host: str | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class HostedExtractorSourceRequest(HostedExtractorSource, RequestResource):
    pass


class HostedExtractorSourceResponse(HostedExtractorSource, ResponseResource[HostedExtractorSourceRequest]):
    created_time: int
    last_updated_time: int

    def as_request_resource(self) -> HostedExtractorSourceRequest:
        return HostedExtractorSourceRequest.model_validate(self.dump(), extra="ignore")
