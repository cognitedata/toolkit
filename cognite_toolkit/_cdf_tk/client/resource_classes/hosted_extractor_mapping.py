from typing import Annotated, Any, ClassVar, Literal

from pydantic import BeforeValidator, ConfigDict

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
    UpdatableRequestResource,
)
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.utils._auxiliary import registry_from_model_classes


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


class UnknownMappingInput(MappingInputDefinition):
    model_config = ConfigDict(extra="allow")
    type: str


def _handle_unknown_mapping_input(value: Any) -> Any:
    if isinstance(value, dict):
        input_type = value.get("type")
        if input_type not in _MAPPING_INPUT_BY_TYPE:
            return UnknownMappingInput.model_validate(value)
        return _MAPPING_INPUT_BY_TYPE[input_type].model_validate(value)
    return value


_MAPPING_INPUT_BY_TYPE = registry_from_model_classes(
    (ProtobufInput, CSVInput, XMLInput, JSONInput),
    type_field="type",
)


MappingInput = Annotated[
    ProtobufInput | CSVInput | XMLInput | JSONInput | UnknownMappingInput,
    BeforeValidator(_handle_unknown_mapping_input),
]


class HostedExtractorMapping(BaseModelObject):
    external_id: str
    mapping: Mapping
    published: bool

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class HostedExtractorMappingRequest(HostedExtractorMapping, UpdatableRequestResource):
    non_nullable_fields: ClassVar[frozenset[str]] = frozenset({"input"})
    input: MappingInput | None = None


class HostedExtractorMappingResponse(HostedExtractorMapping, ResponseResource[HostedExtractorMappingRequest]):
    input: MappingInput
    created_time: int
    last_updated_time: int

    @classmethod
    def request_cls(cls) -> type[HostedExtractorMappingRequest]:
        return HostedExtractorMappingRequest
