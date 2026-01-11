from typing import Literal

from cognite_toolkit._cdf_tk.client.data_classes.base import BaseModelObject, RequestResource, ResponseResource

from .identifiers import ExternalId


class FunctionBase(BaseModelObject):
    """Base class for Function with common fields."""

    external_id: str
    name: str
    description: str | None = None
    owner: str | None = None
    file_id: int | None = None
    function_path: str | None = None
    secrets: dict[str, str] | None = None
    env_vars: dict[str, str] | None = None
    cpu: float | None = None
    memory: float | None = None
    runtime: Literal["py39", "py310", "py311", "py312"] | None = None
    runtime_version: str | None = None
    metadata: dict[str, str] | None = None
    index_url: str | None = None
    extra_index_urls: list[str] | None = None


class FunctionRequest(FunctionBase, RequestResource):
    """Request resource for creating/updating functions."""

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class FunctionStatus(BaseModelObject):
    """Status information for a deployed function."""

    status: Literal["Queued", "Deploying", "Ready", "Failed"] | None = None
    error_message: str | None = None


class FunctionResponse(FunctionBase, ResponseResource[FunctionRequest]):
    """Response resource for functions."""

    id: int
    created_time: int
    status: Literal["Queued", "Deploying", "Ready", "Failed"] | None = None
    error: FunctionStatus | None = None

    def as_request_resource(self) -> FunctionRequest:
        return FunctionRequest.model_validate(self.dump(), extra="ignore")
