from cognite_toolkit._cdf_tk.client.data_classes.base import (
    BaseModelObject,
    RequestResource,
    ResponseResource,
)

from .identifiers import ExternalId


class Credentials(BaseModelObject):
    nonce: str


class HostedExtractorDestination(BaseModelObject):
    external_id: str
    target_data_set_id: int | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class HostedExtractorDestinationRequest(HostedExtractorDestination, RequestResource):
    credentials: Credentials | None = None


class HostedExtractorDestinationResponse(
    HostedExtractorDestination, ResponseResource[HostedExtractorDestinationRequest]
):
    session_id: int | None = None
    created_time: int
    last_updated_time: int

    def as_request_resource(self) -> HostedExtractorDestinationRequest:
        return HostedExtractorDestinationRequest.model_validate(self.dump(), extra="ignore")
