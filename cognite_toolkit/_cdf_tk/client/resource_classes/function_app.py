"""Function apps are Cognite Functions that serve HTTP endpoints.

Based on the API specification at:
https://api-docs.cognite.com/20230101-beta/tag/Function-Apps
"""

from typing import Literal, TypeAlias

from pydantic import Field, JsonValue

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.client.resource_classes.function import FunctionResponse

FunctionType: TypeAlias = Literal["classic", "functionApp"]
FunctionAppCallRuntimeStatus: TypeAlias = Literal["Completed", "Failed", "Timeout"]


class FunctionAppResponse(FunctionResponse):
    """A function as returned by the function apps endpoints."""

    function_type: FunctionType | str


class FunctionAppCallPayload(BaseModelObject):
    """The HTTP request forwarded to the function app.

    ``function_external_id`` selects the function app and is not part of the request body.
    """

    function_external_id: str = Field(exclude=True)
    path: str
    method: str
    body: dict[str, JsonValue] | None = None


class FunctionAppCallError(BaseModelObject):
    """API-level error of a function app call."""

    code: int
    message: str
    missing: list[dict[str, JsonValue]] | None = None
    duplicated: list[dict[str, JsonValue]] | None = None


class FunctionAppCallRuntimeResponse(BaseModelObject):
    """Result of a synchronous function app call."""

    call_id: int
    function_id: int
    runtime_status: FunctionAppCallRuntimeStatus | str
    response: dict[str, JsonValue] | None = None
    error: FunctionAppCallError | None = None
