from typing import Any

from cognite_toolkit._cdf_tk.client.data_classes.base import (
    BaseModelObject,
    Identifier,
    RequestResource,
    ResponseResource,
)


class WorkflowVersionId(Identifier):
    workflow_external_id: str
    version: str

    def __str__(self) -> str:
        return f"workflowExternalId='{self.workflow_external_id}', version='{self.version}'"


class WorkflowVersion(BaseModelObject):
    workflow_external_id: str
    version: str
    workflow_definition: dict[str, Any] | None = None

    def as_id(self) -> WorkflowVersionId:
        return WorkflowVersionId(workflow_external_id=self.workflow_external_id, version=self.version)


class WorkflowVersionRequest(WorkflowVersion, RequestResource):
    pass


class WorkflowVersionResponse(WorkflowVersion, ResponseResource[WorkflowVersionRequest]):
    created_time: int
    last_updated_time: int

    def as_request_resource(self) -> WorkflowVersionRequest:
        return WorkflowVersionRequest.model_validate(self.dump(), extra="ignore")
