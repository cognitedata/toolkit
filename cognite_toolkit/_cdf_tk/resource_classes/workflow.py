from pydantic import Field

from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExternalId

from .base import ToolkitResource


class WorkflowYAML(ToolkitResource):
    external_id: str = Field(
        max_length=255,
        description="Identifier for a workflow. Must be unique for the project."
        " No trailing or leading whitespace and no null characters allowed.",
    )
    description: str | None = Field(None, max_length=500)
    data_set_external_id: str | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)
