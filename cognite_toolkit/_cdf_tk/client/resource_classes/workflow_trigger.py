from typing import Annotated, Any, Literal

from pydantic import BeforeValidator, ConfigDict, JsonValue

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    RequestResource,
    ResponseResource,
)
from cognite_toolkit._cdf_tk.client._types import Metadata
from cognite_toolkit._cdf_tk.client.identifiers import ContainerId, ExternalId
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses


class TriggerRuleDefinition(BaseModelObject):
    trigger_type: str


class ScheduleTriggerRule(TriggerRuleDefinition):
    trigger_type: Literal["schedule"] = "schedule"
    cron_expression: str
    timezone: str | None = None


class DataModelingTriggerRule(TriggerRuleDefinition):
    trigger_type: Literal["dataModeling"] = "dataModeling"
    data_modeling_query: JsonValue
    batch_size: int
    batch_timeout: int


class RecordSource(BaseModelObject):
    source: ContainerId
    properties: list[str]


class RecordStreamTriggerRule(TriggerRuleDefinition):
    trigger_type: Literal["recordStream"] = "recordStream"
    stream_external_id: str
    filter: JsonValue
    sources: list[RecordSource]
    batch_size: int
    batch_timeout: int


class UnknownTriggerRule(TriggerRuleDefinition):
    model_config = ConfigDict(extra="allow")
    trigger_type: str


def _handle_unknown_trigger(value: Any) -> Any:
    if isinstance(value, dict):
        trigger_type = value.get("triggerType")
        if trigger_type not in _TRIGGER_RULE_BY_TYPE:
            return UnknownTriggerRule.model_validate(value)
        else:
            return _TRIGGER_RULE_BY_TYPE[trigger_type].model_validate(value)
    return value


_TRIGGER_RULE_BY_TYPE = {
    cls_.model_fields["trigger_type"].default: cls_
    for cls_ in get_concrete_subclasses(TriggerRuleDefinition)
    if cls_ is not UnknownTriggerRule
}

TriggerRule = Annotated[
    ScheduleTriggerRule | DataModelingTriggerRule | RecordStreamTriggerRule | UnknownTriggerRule,
    BeforeValidator(_handle_unknown_trigger),
]


class NonceCredentials(BaseModelObject):
    nonce: str


class WorkflowTrigger(BaseModelObject):
    external_id: str
    trigger_rule: TriggerRule
    workflow_external_id: str
    workflow_version: str
    input: JsonValue | None = None
    metadata: Metadata | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class WorkflowTriggerRequest(WorkflowTrigger, RequestResource):
    # Note: authentication with nonce is required, but we set it to optional to
    # allow loading from file without it. This is utilized in the WorkflowTriggerCRUD.
    authentication: NonceCredentials | None = None


class WorkflowTriggerResponse(WorkflowTrigger, ResponseResource[WorkflowTriggerRequest]):
    created_time: int
    last_updated_time: int
    is_paused: bool

    @classmethod
    def request_cls(cls) -> type[WorkflowTriggerRequest]:
        return WorkflowTriggerRequest
