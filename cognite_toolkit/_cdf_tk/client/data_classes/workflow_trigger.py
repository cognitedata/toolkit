from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client.data_classes.base import (
    BaseModelObject,
    RequestResource,
    ResponseResource,
)

from .identifiers import ExternalId


class ScheduleTriggerRule(BaseModelObject):
    trigger_type: str | None = None
    cron_expression: str | None = None
    timezone: str | None = None


class DataModelingTriggerRule(BaseModelObject):
    trigger_type: str | None = None
    data_modeling_query: JsonValue | None = None
    batch_size: int | None = None
    batch_timeout: int | None = None


class TriggerRule(BaseModelObject):
    trigger_type: str | None = None
    cron_expression: str | None = None
    timezone: str | None = None
    data_modeling_query: JsonValue | None = None
    batch_size: int | None = None
    batch_timeout: int | None = None


class Authentication(BaseModelObject):
    client_id: str | None = None
    client_secret: str | None = None


class WorkflowTrigger(BaseModelObject):
    external_id: str
    trigger_rule: TriggerRule | None = None
    workflow_external_id: str | None = None
    workflow_version: str | None = None
    input: JsonValue | None = None
    metadata: dict[str, str] | None = None
    authentication: Authentication | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class WorkflowTriggerRequest(WorkflowTrigger, RequestResource): ...


class WorkflowTriggerResponse(WorkflowTrigger, ResponseResource[WorkflowTriggerRequest]):
    created_time: int
    last_updated_time: int

    def as_request_resource(self) -> WorkflowTriggerRequest:
        return WorkflowTriggerRequest.model_validate(self.dump(), extra="ignore")
