from typing import Annotated, Literal

from pydantic import Field, JsonValue

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    RequestResource,
    ResponseResource,
)

from .identifiers import ExternalId


class TriggerRuleDefinition(BaseModelObject):
    trigger_type: str


class ScheduleTriggerRule(BaseModelObject):
    trigger_type: Literal["schedule"] = "schedule"
    cron_expression: str
    timezone: str | None = None


class DataModelingTriggerRule(BaseModelObject):
    trigger_type: Literal["dataModeling"] = "dataModeling"
    data_modeling_query: JsonValue
    batch_size: int
    batch_timeout: int


TriggerRule = Annotated[
    ScheduleTriggerRule | DataModelingTriggerRule,
    Field(discriminator="trigger_type"),
]


class NonceCredentials(BaseModelObject):
    nonce: str


class WorkflowTrigger(BaseModelObject):
    external_id: str
    trigger_rule: TriggerRule
    workflow_external_id: str
    workflow_version: str
    input: JsonValue | None = None
    metadata: dict[str, str] | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class WorkflowTriggerRequest(WorkflowTrigger, RequestResource):
    authentication: NonceCredentials


class WorkflowTriggerResponse(WorkflowTrigger, ResponseResource[WorkflowTriggerRequest]):
    created_time: int
    last_updated_time: int
    is_paused: bool

    def as_request_resource(self) -> WorkflowTriggerRequest:
        return WorkflowTriggerRequest.model_validate(self.dump(), extra="ignore")
