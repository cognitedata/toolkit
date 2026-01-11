from pydantic import Field, JsonValue

from cognite_toolkit._cdf_tk.client.data_classes.base import (
    BaseModelObject,
    Identifier,
    RequestResource,
    ResponseResource,
)


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

    function_id: int
    function_external_id: str | None = Field(None, exclude=True)
    nonce: str

    def as_id(self) -> FunctionScheduleId:
        if self.function_external_id is None:
            raise ValueError("function_external_id must be set to create FunctionScheduleId")
        return FunctionScheduleId(function_external_id=self.function_external_id, name=self.name)


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
