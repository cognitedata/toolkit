from typing import Literal, TypeAlias

from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.client.identifiers import InternalId

FunctionCallStatus: TypeAlias = Literal["Running", "Completed", "Failed", "Timeout", "ConcurrencyLimitExceeded"]


class FunctionCallResponse(BaseModelObject):
    """A single execution of a Cognite Function."""

    id: int
    status: FunctionCallStatus | str
    start_time: int
    function_id: int
    end_time: int | None = None
    error: str | None = None
    schedule_id: int | None = None
    scheduled_time: int | None = None

    def as_id(self) -> InternalId:
        return InternalId(id=self.id)


class FunctionCallLogEntry(BaseModelObject):
    """A single line of stdout or stderr from a function call."""

    timestamp: int | None = None
    message: str | None = None


class FunctionCallLogs(BaseModelObject):
    """Logs produced by a function call."""

    items: list[FunctionCallLogEntry]


class FunctionCallResult(BaseModelObject):
    """Response payload returned by a completed function call."""

    function_id: int
    call_id: int
    response: JsonValue | None = None
