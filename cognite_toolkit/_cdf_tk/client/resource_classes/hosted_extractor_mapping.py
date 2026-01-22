from typing import Annotated, Literal

from pydantic import Field

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
    UpdatableRequestResource,
)

from .identifiers import ExternalId


class Mapping(BaseModelObject):
    expression: str


class MappingInputDefinition(BaseModelObject):
    type: str


class ProtobufFile(BaseModelObject):
    file_name: str
    content: str


class ProtobufInput(BaseModelObject):
    type: Literal["protobuf"] = "protobuf"
    message_name: str
    files: list[ProtobufFile]


class CSVInput(BaseModelObject):
    type: Literal["csv"] = "csv"
    delimiter: str = ","
    custom_keys: list[str] | None = None


class XMLInput(BaseModelObject):
    type: Literal["xml"] = "xml"


class JSONInput(BaseModelObject):
    type: Literal["json"] = "json"


MappingInput = Annotated[
    ProtobufInput | CSVInput | XMLInput | JSONInput,
    Field(discriminator="type"),
]


class HostedExtractorMapping(BaseModelObject):
    external_id: str
    mapping: Mapping
    published: bool

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class HostedExtractorMappingRequest(HostedExtractorMapping, UpdatableRequestResource):
    input: MappingInput | None = None


class HostedExtractorMappingResponse(HostedExtractorMapping, ResponseResource[HostedExtractorMappingRequest]):
    input: MappingInput
    created_time: int
    last_updated_time: int

    def as_request_resource(self) -> HostedExtractorMappingRequest:
        return HostedExtractorMappingRequest.model_validate(self.dump(), extra="ignore")
