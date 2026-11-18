from typing import Any, Literal

from pydantic import model_validator

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.client.resource_classes.signal_subscription import SinkRef

SignalTopicName = Literal[
    "cognite_integrations",
    "cognite_workflows",
    "cognite_hosted_extractors",
    "cognite_sandbox",
    "cognite_comment_notifications",
]


class SignalMetadata(BaseModelObject): ...


class IntegrationSignalMetadata(SignalMetadata):
    extractor_external_id: str | None = None
    extractor_version: str | None = None
    task_name: str | None = None
    start_time: int | None = None
    end_time: int | None = None


class WorkflowSignalMetadata(SignalMetadata):
    execution_start_time: str | None = None
    execution_end_time: str | None = None
    trigger_attempt_fail_time: str | None = None
    trigger_external_id: str | None = None


class HostedExtractorsSignalMetadata(SignalMetadata):
    source_type: str | None = None
    source_external_id: str | None = None
    destination_external_id: str | None = None


class SandboxSignalMetadata(SignalMetadata):
    tag: str | None = None
    detail: str | None = None


class CommentNotificationsSignalMetadata(SignalMetadata):
    activity_name: str | None = None
    plan_id: str | None = None
    canvas_id: str | None = None
    activity_external_id: str | None = None


class UnknownSignalMetadata(SignalMetadata): ...


_METADATA_BY_TOPIC: dict[str, type[SignalMetadata]] = {
    "cognite_integrations": IntegrationSignalMetadata,
    "cognite_workflows": WorkflowSignalMetadata,
    "cognite_hosted_extractors": HostedExtractorsSignalMetadata,
    "cognite_sandbox": SandboxSignalMetadata,
    "cognite_comment_notifications": CommentNotificationsSignalMetadata,
}


class SignalResponse(BaseModelObject):
    """A notification delivered to a sink, for example that an extractor reported an error.

    The type of ``metadata`` is determined by the topic, for example ``IntegrationSignalMetadata`` for integrations.
    """

    external_id: str
    topic: SignalTopicName | str
    sink: SinkRef
    severity: Literal["info", "warning", "error"] | str
    resource_id: str
    category: str
    metadata: (
        IntegrationSignalMetadata
        | WorkflowSignalMetadata
        | HostedExtractorsSignalMetadata
        | SandboxSignalMetadata
        | CommentNotificationsSignalMetadata
        | UnknownSignalMetadata
    )
    message: str
    details: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _metadata_by_topic(cls, data: Any) -> Any:
        if not isinstance(data, dict) or not isinstance(data.get("metadata"), dict):
            return data
        metadata_cls = _METADATA_BY_TOPIC.get(data.get("topic", ""), UnknownSignalMetadata)
        return {**data, "metadata": metadata_cls.model_validate(data["metadata"])}


class SignalSyncResponse(BaseModelObject):
    """A batch of signals. Pass ``next_cursor`` to the next sync call to get the signals that arrived since."""

    items: list[SignalResponse]
    next_cursor: str
    more_data: bool


class SignalTopic(BaseModelObject):
    name: str
    is_global: bool
    categories: list[str]
