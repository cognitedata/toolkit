from typing import Any

from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client.data_classes.base import (
    BaseModelObject,
    RequestResource,
    ResponseResource,
)

from .identifiers import ExternalId


class WorkflowTrigger(BaseModelObject):
    external_id: str
    trigger_rule: dict[str, Any] | None = None
    workflow_external_id: str | None = None
    workflow_version: str | None = None
    input: JsonValue | None = None
    metadata: dict[str, str] | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class WorkflowTriggerRequest(WorkflowTrigger, RequestResource):
    pass


class WorkflowTriggerResponse(WorkflowTrigger, ResponseResource[WorkflowTriggerRequest]):
    created_time: int
    last_updated_time: int

    def as_request_resource(self) -> WorkflowTriggerRequest:
        return WorkflowTriggerRequest.model_validate(self.dump(), extra="ignore")
