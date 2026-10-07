import sys
from typing import Annotated, Literal

from pydantic import Field, JsonValue

from cognite_toolkit._cdf_tk.client.identifiers import ContainerId, ExternalId

from .authentication import AuthenticationClientIdSecret
from .base import BaseModelResource, ToolkitResource

if sys.version_info < (3, 11):
    pass
else:
    pass


class TriggerRuleYAML(BaseModelResource):
    trigger_type: str


class ScheduleTrigger(TriggerRuleYAML):
    trigger_type: Literal["schedule"] = Field("schedule")
    cron_expression: str = Field(
        description="A cron expression (UNIX format) specifying when the trigger should be executed. Use https://crontab.guru/ to create a cron expression. The API may adjust the exact timing of cron job executions to distribute the backend load more evenly. However, it will aim to maintain the overall frequency of executions as specified in the cron expression.",
    )
    timezone: str = Field(
        "UTC",
        description="Specifies the IANA time zone in which the cron expression is evaluated. Time zones must be valid as listed in https://docs.oracle.com/cd/E72987_01/wcs/tag-ref/MISC/TimeZones.html.",
    )


class DataModelingTrigger(TriggerRuleYAML):
    trigger_type: Literal["dataModeling"] = Field("dataModeling")
    data_modeling_query: JsonValue
    batch_size: int = Field(
        ge=100, le=1_000, description="The maximum number of items to pass to a workflow execution."
    )
    batch_timeout: int = Field(
        ge=60,
        le=86_400,
        description="The maximum time in seconds to wait for the batch to be filled before passing it to a workflow execution.",
    )


class RecordSource(BaseModelResource):
    source: ContainerId = Field(description="Reference to a container.")
    properties: list[str] = Field(
        description='Properties to return for the specified container. Use "*" to return all properties.',
        min_length=1,
        max_length=1000,
    )


class RecordStreamTriggerRule(TriggerRuleYAML):
    trigger_type: Literal["recordStream"] = Field("recordStream")
    stream_external_id: str = Field(
        description="The external ID of the stream to subscribe to for record changes.",
        pattern="^[a-z]([a-z0-9_-]{0,98}[a-z0-9])?$",
        min_length=1,
        max_length=100,
    )
    filter: JsonValue | None = None
    sources: list[RecordSource] | None = None
    batch_size: int = Field(
        description="The maximum number of records to pass to a workflow execution.",
        ge=1,
        le=100,
    )
    batch_timeout: int = Field(
        description="The maximum time in seconds to wait for the batch to be filled before passing it to a workflow execution. A partial batch will be passed after the timeout. A full batch will be passed without further delay.",
        ge=10,
        le=86400,
    )


TriggerRule = Annotated[
    ScheduleTrigger | DataModelingTrigger | RecordStreamTriggerRule, Field(discriminator="trigger_type")
]


class WorkflowTriggerYAML(ToolkitResource):
    external_id: str = Field(
        max_length=255,
        description="Identifier for a trigger. Must be unique for the project. "
        "No trailing or leading whitespace and no null characters allowed.",
    )
    trigger_rule: TriggerRule
    input: JsonValue | None = None
    metadata: dict[str, str] | None = None
    workflow_external_id: str = Field(
        max_length=255,
        description="Identifier for a workflow. Must be unique for the project. "
        "No trailing or leading whitespace and no null characters allowed.",
    )
    workflow_version: str = Field(
        max_length=255,
        description="Identifier for a version. Must be unique for the workflow. No trailing or"
        " leading whitespace and no null characters allowed.",
    )
    is_paused: bool | None = Field(
        None,
        description="""Pauses a trigger. When paused, the trigger will not fire until it is resumed.
        Note: For data modeling and records stream triggers, processing continues where the last
        trigger run left off at pause time, not necessarily starting again on the freshest data.
        The cursor can become invalid if paused too long (stream retention applies).""",
    )
    authentication: AuthenticationClientIdSecret = Field(description="Credentials required for the authentication.")

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)
