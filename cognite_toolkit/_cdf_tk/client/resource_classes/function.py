from typing import Literal, TypeAlias

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, RequestResource, ResponseResource

from .identifiers import ExternalId

FunctionStatus: TypeAlias = Literal["Queued", "Deploying", "Ready", "Failed"]


class FunctionBase(BaseModelObject):
    """Base class for Function with common fields."""

    external_id: str | None = None
    name: str
    file_id: int
    description: str | None = None
    owner: str | None = None
    function_path: str | None = None
    secrets: dict[str, str] | None = None
    env_vars: dict[str, str] | None = None
    cpu: float | None = None
    memory: float | None = None
    runtime: Literal["py310", "py311", "py312"] | None = None
    runtime_version: str | None = None
    metadata: dict[str, str] | None = None
    index_url: str | None = None
    extra_index_urls: list[str] | None = None


class FunctionRequest(FunctionBase, RequestResource):
    """Request resource for creating/updating functions."""

    def as_id(self) -> ExternalId:
        if self.external_id is None:
            raise ValueError("Cannot create ExternalId: external_id is None")
        return ExternalId(external_id=self.external_id)


class FunctionAPIError(BaseModelObject):
    code: int
    message: str


class FunctionResponse(FunctionBase, ResponseResource[FunctionRequest]):
    """Response resource for functions."""

    id: int
    created_time: int
    status: FunctionStatus | None = None
    error: FunctionAPIError | None = None

    def as_request_resource(self) -> FunctionRequest:
        return FunctionRequest.model_validate(self.dump(), extra="ignore")
