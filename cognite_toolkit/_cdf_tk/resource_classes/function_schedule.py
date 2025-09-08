from pydantic import Field

from .authentication import AuthenticationClientIdSecret
from .base import ToolkitResource


class FunctionScheduleYAML(ToolkitResource):
    function_external_id: str = Field(
        description="The external ID provided by the client for the function.",
        max_length=255,
    )
    name: str = Field(
        description="The name of the function schedule.",
        min_length=1,
        max_length=140,
    )
    cron_expression: str = Field(
        description="Cron expression.",
        min_length=1,
        max_length=1024,
    )
    description: str | None = Field(
        default=None,
        description="The description of the function schedule.",
        min_length=1,
        max_length=500,
    )
    data: dict[str, object] | None = Field(
        default=None,
        description="Input data to the function.",
    )
    authentication: AuthenticationClientIdSecret | None = Field(
        default=None, description="Credentials required for the authentication."
    )
