from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    RequestResource,
    ResponseResource,
)

from .identifiers import ExternalId


class Workflow(BaseModelObject):
    external_id: str
    description: str | None = None
    data_set_id: int | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class WorkflowRequest(Workflow, RequestResource): ...


class WorkflowResponse(Workflow, ResponseResource[WorkflowRequest]):
    created_time: int
    last_updated_time: int

    def as_request_resource(self) -> WorkflowRequest:
        return WorkflowRequest.model_validate(self.dump(), extra="ignore")
