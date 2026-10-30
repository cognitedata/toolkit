"""Wire models for the pre-GA Function Apps API.

Keep API-specific response details here until Function Apps have a stable SDK model.
"""

from typing import Any, Literal

from pydantic import Field

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, RequestResource, ResponseResource
from cognite_toolkit._cdf_tk.client._types import Metadata
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId

FunctionAppStatus = Literal["queued", "deploying", "ready", "failed"]


class FunctionAppBase(BaseModelObject):
    external_id: str | None = None
    name: str
    file_id: int
    description: str | None = None
    function_path: str | None = None
    secrets: dict[str, str] | None = None
    env_vars: dict[str, str] | None = None
    cpu: float | None = None
    memory: float | None = None
    runtime: str | None = None
    metadata: Metadata | None = None
    index_url: str | None = None
    extra_index_urls: list[str] | None = None


class FunctionAppRequest(FunctionAppBase, RequestResource):
    # This is not part of the request payload. It is retained to determine the
    # Files ACL needed to upload the application bundle.
    data_set_id: int | None = Field(None, exclude=True)

    def as_id(self) -> ExternalId:
        if self.external_id is None:
            raise ValueError("Cannot create ExternalId: external_id is None")
        return ExternalId(external_id=self.external_id)


class FunctionAppAPIError(BaseModelObject):
    """Current alpha error payload returned by the Function Apps backend."""

    message: str | None = None
    code: str | None = None
    details: dict[str, Any] | list[Any] | str | None = None


class FunctionAppResponse(FunctionAppBase, ResponseResource[FunctionAppRequest]):
    id: int
    created_time: int
    owner: str | None = None
    status: FunctionAppStatus | None = None
    error: FunctionAppAPIError | None = None

    @classmethod
    def request_cls(cls) -> type[FunctionAppRequest]:
        return FunctionAppRequest
