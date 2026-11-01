from pathlib import Path
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Discriminator, Field, FilePath, Tag, field_validator, model_validator

from cognite_toolkit._cdf_tk.client.identifiers import DataModelId
from cognite_toolkit._cdf_tk.constants import DM_EXTERNAL_ID_PATTERN, DM_VERSION_PATTERN, SPACE_FORMAT_PATTERN
from cognite_toolkit._cdf_tk.yaml_classes import ToolkitResource

from ._table import DMSContainer, DMSEnum, DMSNode, DMSProperty, DMSView, normalize_table_names


class NeatMetadata(BaseModel):
    space: str = Field(
        description="The Space identifier (id).",
        min_length=1,
        max_length=43,
        pattern=SPACE_FORMAT_PATTERN,
    )
    external_id: str = Field(
        description="External-id of the data model.",
        min_length=1,
        max_length=255,
        pattern=DM_EXTERNAL_ID_PATTERN,
    )
    version: str = Field(
        description="Version of the data model.",
        max_length=43,
        pattern=DM_VERSION_PATTERN,
    )
    name: str | None = Field(
        default=None,
        description="Human readable name for the data model.",
        max_length=255,
    )
    description: str | None = Field(
        default=None,
        description="Description of the data model.",
        max_length=1024,
    )
    creator: str | None = Field(None, description="The creator of the data model.", max_length=255)
    governed_spaces: list[str] | None = Field(
        None, description="List of spaces governed by the data model.", max_length=43
    )

    @model_validator(mode="before")
    @classmethod
    def _key_value_pairs(cls, data: Any) -> Any:
        """Convert a list of key-value pairs to a dict."""
        if isinstance(data, list):
            return {
                item["key"]: item["value"]
                for item in data
                if isinstance(item, dict) and "key" in item and "value" in item
            }
        return data


class NeatYAML(ToolkitResource):
    """A data model described with the NEAT table YAML format."""

    metadata: NeatMetadata = Field(validation_alias="Metadata")
    properties: list[DMSProperty] = Field(validation_alias="Properties")
    views: list[DMSView] = Field(validation_alias="Views")
    containers: list[DMSContainer] = Field(default_factory=list, validation_alias="Containers")
    enum: list[DMSEnum] = Field(default_factory=list, validation_alias="Enum")
    nodes: list[DMSNode] = Field(default_factory=list, validation_alias="Nodes")

    @model_validator(mode="before")
    @classmethod
    def _title_case_keys(cls, data: Any) -> Any:
        return normalize_table_names(data)

    def as_id(self) -> DataModelId:
        """Return the data model identifier declared in the Metadata table."""
        return DataModelId(
            space=self.metadata.space, external_id=self.metadata.external_id, version=self.metadata.version
        )


class NeatSpreadsheet(ToolkitResource):
    model_config = ConfigDict(populate_by_name=True)

    spreadsheet: FilePath

    @field_validator("spreadsheet")
    @classmethod
    def validate_spreadsheet_suffix(cls, value: Path) -> Path:
        if value.suffix.casefold() != ".xlsx":
            raise ValueError("Spreadsheet must reference an .xlsx file.")
        return value

    def as_id(self) -> DataModelId:
        # Todo: read the spreadsheet and extract the metadata to return a DataModelId. This is not implemented yet.
        raise NotImplementedError()


def _discriminate_neat_format(value: Any) -> str | None:
    if isinstance(value, NeatYAML):
        return "yaml"
    if isinstance(value, NeatSpreadsheet):
        return "spreadsheet"
    if not isinstance(value, dict):
        return None
    keys = {key.casefold() for key in value if isinstance(key, str)}
    if "spreadsheet" in keys:
        return "spreadsheet"
    if "metadata" in keys:
        return "yaml"
    # Returning None makes pydantic raise the custom 'invalid_neat_format' error.
    return None


NeatFormat = Annotated[
    Annotated[NeatYAML, Tag("yaml")] | Annotated[NeatSpreadsheet, Tag("spreadsheet")],
    Discriminator(
        _discriminate_neat_format,
        custom_error_type="invalid_neat_format",
        custom_error_message="Invalid NEAT format. Expected either a 'Metadata' key (YAML format) or a "
        "'spreadsheet' key (spreadsheet format).",
    ),
]
