from typing import Literal, TypeAlias

from pydantic import Field

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, RequestResource, ResponseResource
from cognite_toolkit._cdf_tk.client._types import Metadata
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId

FunctionStatus: TypeAlias = Literal["Queued", "Deploying", "Ready", "Failed", "Retired"]


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
    runtime: Literal["py38", "py39", "py310", "py311", "py312"] | None = None
    metadata: Metadata | None = None
    index_url: str | None = None
    extra_index_urls: list[str] | None = None


class FunctionRequest(FunctionBase, RequestResource):
    """Request resource for creating/updating functions."""

    # This is not part of the request payload, but we store the information such that we can use it
    # to check which acl is needed to deploy the function code.
    data_set_id: int | None = Field(None, exclude=True)

    def as_id(self) -> ExternalId:
        if self.external_id is None:
            raise ValueError("Cannot create ExternalId: external_id is None")
        return ExternalId(external_id=self.external_id)


class FunctionAPIError(BaseModelObject):
    trace: str
    message: str


class FunctionResponse(FunctionBase, ResponseResource[FunctionRequest]):
    """Response resource for functions."""

    id: int
    created_time: int
    runtime_version: str | None = None
    status: FunctionStatus | None = None
    error: FunctionAPIError | None = None

    @classmethod
    def request_cls(cls) -> type[FunctionRequest]:
        return FunctionRequest
FunctionRuntime: TypeAlias = Literal["py38", "py39", "py310", "py311", "py312"]


class ResourceLimit(BaseModelObject):
    """Resource limit with min, max, and default values."""

    min: int | float
    max: int | float
    default: int | float


class FunctionLimits(BaseModelObject):
    """Function limits for a CDF project."""

    timeout_minutes: int
    cpu_cores: ResourceLimit
    memory_gb: ResourceLimit
    runtimes: list[FunctionRuntime]
    response_size_mb: int
