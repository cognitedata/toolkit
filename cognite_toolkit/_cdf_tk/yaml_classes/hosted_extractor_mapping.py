from typing import Annotated, Literal

from pydantic import Field

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId

from .base import BaseModelResource, ToolkitResource


class Mapping(BaseModelResource):
    expression: str = Field(
        description="Custom transform expression written in the Cognite transformation language.", max_length=2000
    )


class MappingInput(BaseModelResource):
    type: str


class JsonMappingInput(MappingInput):
    type: Literal["json"] = Field("json")


class XMLMappingInput(MappingInput):
    type: Literal["xml"] = Field("xml")


class CSVMappingInput(MappingInput):
    type: Literal["csv"] = Field("csv")
    delimiter: str = Field(
        description="A single ASCII character used as the separator in the CSV file.",
        min_length=1,
        max_length=1,
        default=",",
    )
    skip: int = Field(0, description="Undocumented, but is returned in the response form the API.")
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
    type: Literal["protobuf"] = Field("protobuf")
    message_name: str = Field(description="Name of root message in the protobuf files.", max_length=128)
    files: list[ProtobufFile] = Field(description="The protobuf schema in text format.", max_length=5000)


MappingInputType = Annotated[
    JsonMappingInput | XMLMappingInput | CSVMappingInput | ProtobufMappingInput,
    Field(discriminator="type"),
]


class HostedExtractorMappingYAML(ToolkitResource):
    external_id: str = Field(
        description="The external ID provided by the client. Must be unique for the resource type.", max_length=255
    )
    mapping: Mapping
    input: MappingInputType | None = Field(None, description="The input format of the data to be transformed.")
    published: bool = Field(description="Whether this mapping is published and should be available to be used in jobs.")

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)
