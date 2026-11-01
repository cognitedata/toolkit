from typing import Any

from pydantic import Field, model_validator

from cognite_toolkit._cdf_tk.client.identifiers import DataModelId
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
