from pydantic import Field
from pydantic.config import JsonValue

from cognite_toolkit._cdf_tk.client.identifiers import DataModelId
from cognite_toolkit._cdf_tk.yaml_classes import ToolkitResource


class NeatYAML(ToolkitResource):
    """A YAML resource representing a NEAT configuration."""

    metadata: list[dict[str, JsonValue]]
    properties: list[dict[str, JsonValue]]
    views: list[dict[str, JsonValue]]
    containers: list[dict[str, JsonValue]] = Field(default_factory=list)
    enum: list[dict[str, JsonValue]] = Field(default_factory=list)
    nodes: list[dict[str, JsonValue]] = Field(default_factory=list)

    def as_id(self) -> DataModelId:
        """Returns the identifier of the NEAT resource."""
        raise NotImplementedError()
