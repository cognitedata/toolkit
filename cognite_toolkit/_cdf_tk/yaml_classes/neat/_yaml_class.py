from pathlib import Path
from typing import Annotated, Any

from pydantic import Discriminator, Field, Tag, model_validator

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.identifiers import DataModelId
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.yaml_classes import ToolkitResource

from ._table import DMSContainer, DMSEnum, DMSNode, DMSProperty, DMSView, MetadataValue, normalize_table_names


class NeatYAML(ToolkitResource):
    """A data model described with the NEAT table YAML format."""

    metadata: list[MetadataValue] = Field(validation_alias="Metadata")
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
        metadata = {item.key: item.value for item in self.metadata}
        try:
            return DataModelId(
                space=str(metadata["space"]),
                external_id=str(metadata["externalId"]),
                version=str(metadata["version"]),
            )
        except KeyError as error:
            raise ValueError(f"Metadata table is missing required value {error.args[0]!r}.") from error


class NeatSpreadsheet(ToolkitResource):
    spreadsheet: Path
    space: str
    external_id: str
    version: str

    def as_id(self) -> Identifier:
        raise ToolkitValueError("NeatSpreadsheet does not have an identifier.")


def _discriminate_neat_format(value: Any) -> str:
    if not isinstance(value, dict):
        raise ToolkitValueError(
            f"Invalid NEAT format. Expected either a YAML file or a spreadsheet file, got {type(value).__name__}."
        )
    if "spreadsheet" in value:
        return "spreadsheet"
    if "metadata" in value:
        return "yaml"
    raise ToolkitValueError(
        "Invalid NEAT format. Expected either a YAML file or a spreadsheet file, but could not find the required keys 'metadata' or 'spreadsheet'."
    )


NeatFormat = Annotated[
    Annotated[NeatYAML, Tag("yaml")],
    Annotated[NeatSpreadsheet, Tag("spreadsheet")],
    Discriminator(
        _discriminate_neat_format,
        custom_error_type="invalid_neat_format",
        custom_error_message="Invalid NEAT format. Expected either a YAML file or a spreadsheet file.",
    ),
]
