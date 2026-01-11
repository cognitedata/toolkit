from typing import Any

from cognite_toolkit._cdf_tk.client.data_classes.base import (
    BaseModelObject,
    RequestResource,
    ResponseResource,
)

from .identifiers import ExternalId


class HostedExtractorMapping(BaseModelObject):
    external_id: str
    mapping: dict[str, Any] | None = None
    input: dict[str, Any] | None = None
    published: bool | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class HostedExtractorMappingRequest(HostedExtractorMapping, RequestResource):
    pass


class HostedExtractorMappingResponse(HostedExtractorMapping, ResponseResource[HostedExtractorMappingRequest]):
    created_time: int
    last_updated_time: int

    def as_request_resource(self) -> HostedExtractorMappingRequest:
        return HostedExtractorMappingRequest.model_validate(self.dump(), extra="ignore")
