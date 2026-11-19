from typing import Annotated, Literal

from pydantic import Field, field_validator
from pydantic.alias_generators import to_camel
from pydantic_core.core_schema import ValidationInfo

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId

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
    task_name: str | None = Field(
        default=None,
        description="Only include signals about this task. Required if startTime or endTime is set.",
    )
    start_time: int | None = Field(default=None, description="Epoch milliseconds.")
    end_time: int | None = Field(default=None, description="Epoch milliseconds.")

    @field_validator("start_time", "end_time", mode="after")
    @classmethod
    def task_name_required_with_time(cls, val: int | None, info: ValidationInfo) -> int | None:
        if val is not None and info.data.get("task_name") is None:
            raise ValueError(f"taskName must be set when {to_camel(info.field_name or '')} is set.")
        return val


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
    source_type: str | None = None
    source_external_id: str | None = Field(default=None, min_length=1, max_length=255)
    destination_external_id: str | None = Field(default=None, min_length=1, max_length=255)


class SandboxFilterYAML(BaseModelResource):
    topic: Literal["cognite_sandbox"]
    resource: str | None = Field(default=None, min_length=1, max_length=512)
    category: list[str] | None = None
    severity: Literal["info", "warning", "error"] | None = None
    tag: str | None = None


class CommentNotificationsFilterYAML(BaseModelResource):
    topic: Literal["cognite_comment_notifications"]
    resource: str | None = Field(default=None, min_length=1, max_length=512)
    category: list[str] | None = None
    severity: Literal["info", "warning", "error"] | None = None


SubscriptionFilterYAML = Annotated[
    IntegrationsFilterYAML
    | WorkflowsFilterYAML
    | HostedExtractorsFilterYAML
    | SandboxFilterYAML
    | CommentNotificationsFilterYAML,
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

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)
