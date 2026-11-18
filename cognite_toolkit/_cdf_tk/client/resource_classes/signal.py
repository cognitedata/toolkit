from typing import Literal

from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.client.resource_classes.signal_subscription import SinkRef

SignalTopicName = Literal[
    "cognite_integrations",
    "cognite_workflows",
    "cognite_hosted_extractors",
    "cognite_sandbox",
    "cognite_comment_notifications",
]


class SignalResponse(BaseModelObject):
    """A notification delivered to a sink, for example that an extractor reported an error.

    The keys of ``metadata`` depend on the topic, for example ``extractorExternalId`` for integrations.
    """

    external_id: str
    topic: str
    sink: SinkRef
    severity: Literal["info", "warning", "error"]
    resource_id: str
    category: str
    metadata: dict[str, JsonValue]
    message: str
    details: str | None = None


class SignalSyncResponse(BaseModelObject):
    """A batch of signals. Pass ``next_cursor`` to the next sync call to get the signals that arrived since."""

    items: list[SignalResponse]
    next_cursor: str
    more_data: bool


class SignalTopic(BaseModelObject):
    name: str
    is_global: bool
    categories: list[str]
