import sys
from types import MappingProxyType
from typing import Any, ClassVar, cast

from cognite.client.data_classes import WorkflowTrigger
from pydantic import Field, JsonValue, ModelWrapValidatorHandler, model_serializer, model_validator
from pydantic_core.core_schema import SerializationInfo, SerializerFunctionWrapHandler

from cognite_toolkit._cdf_tk.utils import humanize_collection

from .authentication import AuthenticationClientIdSecret
from .base import BaseModelResource, ToolkitResource

if sys.version_info < (3, 11):
    from typing_extensions import Self
else:
    from typing import Self


class TriggerRuleYAML(BaseModelResource):
    _trigger_type: ClassVar[str]

    @model_validator(mode="wrap")
    @classmethod
    def find_trigger_type(
        cls, data: "dict[str, Any] | TriggerRuleYAML", handler: ModelWrapValidatorHandler[Self]
    ) -> Self:
        if isinstance(data, TriggerRuleYAML):
            return cast(Self, data)
        if not isinstance(data, dict):
            raise ValueError(f"Invalid trigger rule data '{type(data)}' expected dict")

        if cls is not TriggerRuleYAML:
            # We are already in a subclass, so just validate as usual
            return handler(data)
        # If not we need to find the right subclass based on the triggerType field.
        if "triggerType" not in data:
            raise ValueError("Invalid trigger rule data missing 'triggerType' key")
        trigger_type = data["triggerType"]
        if trigger_type not in _TRIGGER_CLS_BY_NAME:
            raise ValueError(
                f"invalid trigger type '{trigger_type}'. Expected one of {humanize_collection(_TRIGGER_CLS_BY_NAME.keys(), bind_word='or')}"
            )
        cls_ = _TRIGGER_CLS_BY_NAME[trigger_type]
        return cast(Self, cls_.model_validate({k: v for k, v in data.items() if k != "triggerType"}))

    @model_serializer(mode="wrap", when_used="always", return_type=dict)
    def include_trigger_type(self, handler: SerializerFunctionWrapHandler) -> dict:
        if self._trigger_type is None:
            raise ValueError("Trigger type is not set")
        serialized_data = handler(self)
        serialized_data["triggerType"] = self._trigger_type
        return serialized_data


class ScheduleTrigger(TriggerRuleYAML):
    _trigger_type = "schedule"
    cron_expression: str = Field(
        description="A cron expression (UNIX format) specifying when the trigger should be executed. Use https://crontab.guru/ to create a cron expression. The API may adjust the exact timing of cron job executions to distribute the backend load more evenly. However, it will aim to maintain the overall frequency of executions as specified in the cron expression.",
    )
    timezone: str = Field(
        "UTC",
        description="Specifies the IANA time zone in which the cron expression is evaluated. Time zones must be valid as listed in https://docs.oracle.com/cd/E72987_01/wcs/tag-ref/MISC/TimeZones.html.",
    )


class DataModelingTrigger(TriggerRuleYAML):
    _trigger_type = "dataModeling"
    data_modeling_query: JsonValue
    batch_size: int = Field(
        ge=100, le=1_000, description="The maximum number of items to pass to a workflow execution."
    )
    batch_timeout: int = Field(
        ge=60,
        le=86_400,
        description="The maximum time in seconds to wait for the batch to be filled before passing it to a workflow execution.",
    )


class WorkflowTriggerYAML(ToolkitResource):
    _cdf_resource = WorkflowTrigger
    external_id: str = Field(
        max_length=255,
        description="Identifier for a trigger. Must be unique for the project. "
        "No trailing or leading whitespace and no null characters allowed.",
    )
    trigger_rule: TriggerRuleYAML
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
    authentication: AuthenticationClientIdSecret = Field(description="Credentials required for the authentication.")

    @model_serializer(mode="wrap")
    def serialize_trigger_rules(self, handler: SerializerFunctionWrapHandler, info: SerializationInfo) -> dict:
        # Trigger rules are serialized as empty dicts [{}, {}, ...]
        # This issue arises because Pydantic's serialization mechanism doesn't automatically
        # handle polymorphic serialization for subclasses of TriggerRuleYAML.
        # To address this, we include the below to explicitly calling model dump on the trigger rule
        serialized_data = handler(self)
        serialized_data["triggerRule"] = self.trigger_rule.model_dump(**vars(info))
        return serialized_data


_TRIGGER_CLS_BY_NAME: MappingProxyType[str, type[TriggerRuleYAML]] = MappingProxyType(
    {s._trigger_type: s for s in TriggerRuleYAML.__subclasses__()}
)
