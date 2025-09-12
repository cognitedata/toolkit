import sys
from types import MappingProxyType
from typing import Any, ClassVar, cast

from pydantic import Field, ModelWrapValidatorHandler, model_serializer, model_validator
from pydantic_core.core_schema import SerializationInfo, SerializerFunctionWrapHandler

from cognite_toolkit._cdf_tk.utils import humanize_collection

from .base import BaseModelResource, ToolkitResource

if sys.version_info < (3, 11):
    from typing_extensions import Self
else:
    from typing import Self


class Mapping(BaseModelResource):
    expression: str = Field(
        description="Custom transform expression written in the Cognite transformation language.", max_length=2000
    )


class MappingInput(BaseModelResource):
    type: ClassVar[str]

    @model_validator(mode="wrap")
    @classmethod
    def find_type(cls, data: "dict[str, Any] | MappingInput", handler: ModelWrapValidatorHandler[Self]) -> Self:
        if isinstance(data, MappingInput):
            return cast(Self, data)
        if not isinstance(data, dict):
            raise ValueError(f"Invalid input type data '{type(data)}' expected dict")

        if cls is not MappingInput:
            # We are already in a subclass, so just validate as usual
            return handler(data)
        # If not we need to find the right subclass based on the type field.
        if "type" not in data:
            raise ValueError("Invalid input data missing 'type' key")
        type_ = data["type"]
        if type_ not in _MAPPING_INPUT_CLS_BY_TYPE:
            raise ValueError(
                f"invalid type '{type_}'. Expected one of {humanize_collection(_MAPPING_INPUT_CLS_BY_TYPE.keys(), bind_word='or')}"
            )
        cls_ = _MAPPING_INPUT_CLS_BY_TYPE[type_]
        return cast(Self, cls_.model_validate({k: v for k, v in data.items() if k != "type"}))

    @model_serializer(mode="wrap", when_used="always", return_type=dict)
    def include_type(self, handler: SerializerFunctionWrapHandler) -> dict:
        serialized_data = handler(self)
        serialized_data["type"] = self.type
        return serialized_data


class JsonMappingInput(MappingInput):
    type = "json"


class XMLMappingInput(MappingInput):
    type = "xml"


class CSVMappingInput(MappingInput):
    type = "csv"
    delimiter: str = Field(
        description="A single ASCII character used as the separator in the CSV file.",
        min_length=1,
        max_length=1,
        default=",",
    )
    custom_keys: list[str] | None = Field(
        None,
        description="List of headers. If this is not set, the headers will be retrieved from the CSV file.",
        min_length=1,
        max_length=20,
    )


class ProtobufFile(BaseModelResource):
    file_name: str = Field(
        description="Name of protobuf file. Must contain only letters, numbers, underscores, and hyphens, and must end with '.proto'.",
        max_length=128,
        pattern=r"[a-zA-Z0-9_-]+\.proto",
    )
    content: str = Field(description="Protobuf file content. Must be a valid protobuf file.", max_length=10000)


class ProtobufMappingInput(MappingInput):
    type = "protobuf"
    message_name: str = Field(description="Name of root message in the protobuf files.", max_length=128)
    files: list[ProtobufFile] = Field(description="The protobuf schema in text format.", max_length=5000)


class HostedExtractorMappingYAML(ToolkitResource):
    external_id: str = Field(
        description="The external ID provided by the client. Must be unique for the resource type.", max_length=255
    )
    mapping: Mapping
    input: MappingInput | None = Field(None, description="The input format of the data to be transformed.")
    published: bool = Field(description="Whether this mapping is published and should be available to be used in jobs.")

    @model_serializer(mode="wrap")
    def serialize_input(self, handler: SerializerFunctionWrapHandler, info: SerializationInfo) -> dict:
        # Input is serialized as empty dict
        # This issue arises because Pydantic's serialization mechanism doesn't automatically
        # handle polymorphic serialization for subclasses of MappingInput.
        # To address this, we include the below to explicitly calling model dump on the input
        serialized_data = handler(self)
        if self.input is not None:
            serialized_data["input"] = self.input.model_dump(**vars(info))
        return serialized_data


_MAPPING_INPUT_CLS_BY_TYPE: MappingProxyType[str, type[MappingInput]] = MappingProxyType(
    {s.type: s for s in MappingInput.__subclasses__()}
)
