from typing import Any

from pydantic import Field, JsonValue, model_validator

from cognite_toolkit._cdf_tk.client.identifiers import DataModelId
from cognite_toolkit._cdf_tk.yaml_classes import ToolkitResource


class NeatYAML(ToolkitResource):
    """A data model described with the NEAT table YAML format."""

    metadata: list[dict[str, JsonValue]]
    properties: list[dict[str, JsonValue]]
    views: list[dict[str, JsonValue]]
    containers: list[dict[str, JsonValue]] = Field(default_factory=list)
    enum: list[dict[str, JsonValue]] = Field(default_factory=list)
    nodes: list[dict[str, JsonValue]] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def normalize_table_names(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        return {key.casefold() if isinstance(key, str) else key: value for key, value in data.items()}

    def as_id(self) -> DataModelId:
        """Return the data model identifier declared in the Metadata table."""
        metadata = {str(item["Key"]): item["Value"] for item in self.metadata if "Key" in item and "Value" in item}
        try:
            return DataModelId(
                space=str(metadata["space"]),
                external_id=str(metadata["externalId"]),
                version=str(metadata["version"]),
            )
        except KeyError as error:
            raise ValueError(f"Metadata table is missing required value {error.args[0]!r}.") from error
