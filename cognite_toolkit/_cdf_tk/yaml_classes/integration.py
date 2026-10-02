from pydantic import Field, JsonValue

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId

from .base import BaseModelResource, ToolkitResource


class IntegrationExtractorYAML(BaseModelResource):
    """Extractor that an integration runs."""

    external_id: str = Field(description="External ID of the extractor.", min_length=1)
    version: str | None = Field(default=None, description="Extractor version.")


class IntegrationYAML(ToolkitResource):
    """An integration registered with the Integrations API."""

    external_id: str = Field(
        description="External ID that uniquely identifies the integration.",
        min_length=1,
    )
    extractor: IntegrationExtractorYAML = Field(description="Extractor that this integration runs.")
    name: str | None = Field(default=None, description="Human-readable name of the integration.")
    description: str | None = Field(default=None, description="Description of the integration.")
    documentation: str | None = Field(default=None, description="Documentation for the integration.")
    metadata: dict[str, str] | None = Field(default=None, description="Custom, application specific metadata.")
    allowed_not_seen_minutes: int | None = Field(
        default=None,
        description="Minutes the integration can go without being seen before it is considered down.",
        ge=0,
    )

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class IntegrationConfigYAML(ToolkitResource):
    """A configuration revision for an integration.

    Creating this resource stores a new revision. The API does not update an existing revision.
    """

    external_id: str = Field(
        description="External ID of the integration this configuration revision belongs to.",
        min_length=1,
    )
    config: str | dict[str, JsonValue] = Field(
        description="Configuration content. A YAML mapping or a string that the extractor parses."
    )
    description: str | None = Field(default=None, description="Description of this configuration revision.")

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)
