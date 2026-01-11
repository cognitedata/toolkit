from cognite_toolkit._cdf_tk.client.data_classes.base import (
    BaseModelObject,
    RequestResource,
    ResponseResource,
)

from .identifiers import ExternalId


class Mapping(BaseModelObject):
    expression: str


class ProtobufFile(BaseModelObject):
    file_name: str
    content: str


class MappingInput(BaseModelObject):
    type: str | None = None
    delimiter: str | None = None
    custom_keys: list[str] | None = None
    message_name: str | None = None
    files: list[ProtobufFile] | None = None


class HostedExtractorMapping(BaseModelObject):
    external_id: str
    mapping: Mapping | None = None
    input: MappingInput | None = None
    published: bool | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class HostedExtractorMappingRequest(HostedExtractorMapping, RequestResource): ...


class HostedExtractorMappingResponse(HostedExtractorMapping, ResponseResource[HostedExtractorMappingRequest]):
    created_time: int
    last_updated_time: int

    def as_request_resource(self) -> HostedExtractorMappingRequest:
        return HostedExtractorMappingRequest.model_validate(self.dump(), extra="ignore")
