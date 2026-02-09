from pydantic import Field, JsonValue

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    Identifier,
    RequestResource,
    ResponseResource,
)
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import InternalId


class FunctionScheduleId(Identifier):
    """Identifier for a function schedule."""

    function_external_id: str
    name: str

    def __str__(self) -> str:
        return f"functionExternalId='{self.function_external_id}', name='{self.name}'"


class FunctionScheduleCredentials(BaseModelObject):
    """Credentials for function schedule authentication."""

    client_id: str
    client_secret: str


class FunctionSchedule(BaseModelObject):
    """Base class for function schedule with common fields."""

    name: str
    cron_expression: str
    description: str | None = None
    data: dict[str, JsonValue] | None = None


class FunctionScheduleRequest(FunctionSchedule, RequestResource):
    """Request resource for creating/updating function schedules."""

    # The 'id' field is not part of the request when creating a new resource,
    # but is needed when deleting an existing resource.
    id: int | None = Field(default=None, exclude=True)
    # function_id is set at creation time after looking up the function by external_id
    function_id: int | None = None
    function_external_id: str | None = Field(None, exclude=True)
    # nonce is set at creation time after creating a session
    nonce: str | None = None

    def as_id(self) -> InternalId:
        if self.id is None:
            raise ValueError("Cannot convert FunctionScheduleRequest to InternalId when id is None")
        return InternalId(id=self.id)


class FunctionScheduleResponse(FunctionSchedule, ResponseResource[FunctionScheduleRequest]):
    """Response resource for function schedules."""

    id: int
    created_time: int
    when: str
    function_id: int | None = None
    function_external_id: str | None = None
    session_id: int | None = None

    def as_request_resource(self) -> FunctionScheduleRequest:
        return FunctionScheduleRequest.model_validate(self.dump(), extra="ignore")
