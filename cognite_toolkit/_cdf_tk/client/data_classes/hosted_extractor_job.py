from typing import Any

from cognite_toolkit._cdf_tk.client.data_classes.base import (
    BaseModelObject,
    RequestResource,
    ResponseResource,
)

from .identifiers import ExternalId


class HostedExtractorJob(BaseModelObject):
    external_id: str
    destination_id: str | None = None
    source_id: str | None = None
    format: dict[str, Any] | None = None
    config: dict[str, Any] | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class HostedExtractorJobRequest(HostedExtractorJob, RequestResource):
    pass


class HostedExtractorJobResponse(HostedExtractorJob, ResponseResource[HostedExtractorJobRequest]):
    status: str | None = None
    created_time: int
    last_updated_time: int

    def as_request_resource(self) -> HostedExtractorJobRequest:
        return HostedExtractorJobRequest.model_validate(self.dump(), extra="ignore")
