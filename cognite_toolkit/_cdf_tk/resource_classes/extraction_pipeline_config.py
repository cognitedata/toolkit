from pydantic import Field, JsonValue

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId

from .base import ToolkitResource


class ExtractionPipelineConfigYAML(ToolkitResource):
    external_id: str = Field(
        description="External ID of the extraction pipeline this configuration revision belongs to.",
        min_length=1,
        max_length=255,
    )
    description: str | None = Field(None, description="A description of this configuration revision.")
    config: str | dict[str, JsonValue] = Field(
        description="Configuration content. Can be either a JSON string or a dictionary."
    )

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)
