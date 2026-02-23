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
    max_concurrent_executions: int | None = Field(
        default=None,
        description="Maximum concurrent executions for this workflow. Defaults to the project limit if not specified or explicitly set to None. Values exceeding the project limit are dynamically capped at runtime.",
    )

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)
