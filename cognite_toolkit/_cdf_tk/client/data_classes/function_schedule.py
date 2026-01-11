from pydantic import JsonValue

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


class FunctionScheduleBase(BaseModelObject):
    """Base class for function schedule with common fields."""

    function_external_id: str
    name: str
    cron_expression: str
    description: str | None = None
    data: dict[str, JsonValue] | None = None


class FunctionScheduleRequest(FunctionScheduleBase, RequestResource):
    """Request resource for creating/updating function schedules."""

    authentication: FunctionScheduleCredentials | None = None

    def as_id(self) -> FunctionScheduleId:
        return FunctionScheduleId(function_external_id=self.function_external_id, name=self.name)


class FunctionScheduleResponse(FunctionScheduleBase, ResponseResource[FunctionScheduleRequest]):
    """Response resource for function schedules."""

    id: int
    created_time: int
    function_id: int | None = None
    session_id: int | None = None

    def as_request_resource(self) -> FunctionScheduleRequest:
        return FunctionScheduleRequest.model_validate(self.dump(), extra="ignore")
