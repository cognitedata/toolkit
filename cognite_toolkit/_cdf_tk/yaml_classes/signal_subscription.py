from typing import Annotated, Literal

from pydantic import Field

from cognite_toolkit._cdf_tk.client.identifiers import SignalSubscriptionId

from .base import BaseModelResource, ToolkitResource


class SinkRefWithExternalIdYAML(BaseModelResource):
    type: Literal["email", "user"] = Field(
        description="The type of sink to deliver signals to.",
    )
    external_id: str = Field(
        description="The external ID of the sink.",
        min_length=1,
        max_length=255,
    )


class CurrentUserSinkRefYAML(BaseModelResource):
    type: Literal["current_user"] = Field(
        description="The type of sink to deliver signals to.",
    )


SinkRefYAML = Annotated[
    SinkRefWithExternalIdYAML | CurrentUserSinkRefYAML,
    Field(discriminator="type"),
]


class IntegrationsFilterYAML(BaseModelResource):
    topic: Literal["cognite_integrations"]
    resource: str | None = Field(default=None, min_length=1, max_length=512)
    category: list[str] | None = None
    severity: Literal["info", "warning", "error"] | None = None
    extractor_external_id: str | None = Field(default=None, min_length=1, max_length=255)
    extractor_version: str | None = Field(default=None, min_length=1, max_length=32)


class WorkflowsFilterYAML(BaseModelResource):
    topic: Literal["cognite_workflows"]
    resource: str | None = Field(default=None, min_length=1, max_length=512)
    category: list[str] | None = None
    severity: Literal["info", "warning", "error"] | None = None


class HostedExtractorsFilterYAML(BaseModelResource):
    topic: Literal["cognite_hosted_extractors"]
    resource: str | None = Field(default=None, min_length=1, max_length=512)
    category: list[str] | None = None
    severity: Literal["info", "warning", "error"] | None = None


SubscriptionFilterYAML = Annotated[
    IntegrationsFilterYAML | WorkflowsFilterYAML | HostedExtractorsFilterYAML,
    Field(discriminator="topic"),
]


class SignalSubscriptionYAML(ToolkitResource):
    external_id: str = Field(
        description="The external ID of the subscription.",
        min_length=1,
        max_length=255,
    )
    sink: SinkRefYAML = Field(
        description="Reference to the sink that will receive signals.",
    )
    filter: SubscriptionFilterYAML = Field(
        description="Filter determining which signals are delivered to the sink.",
    )

    def as_id(self) -> SignalSubscriptionId:
        return SignalSubscriptionId(external_id=self.external_id)
