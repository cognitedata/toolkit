from pydantic import Field

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId

from .base import ToolkitResource


class RuleSetYAML(ToolkitResource):
    external_id: str = Field(
        description="User-defined unique identifier for the rule set.",
        min_length=1,
        max_length=100,
        pattern=r"^[a-z]([a-z0-9_-]{0,98}[a-z0-9])?$",
    )
    name: str = Field(
        description="Human-readable name of the rule set.",
        min_length=1,
        max_length=100,
    )

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)
