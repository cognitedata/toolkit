from croniter import croniter
from pydantic import Field, field_validator

from .base import ToolkitResource


class FunctionAuthenticationField(ToolkitResource):
    client_id: str = Field(description="Client Id.")
    client_secret: str = Field(description="Client Secret.")


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
    function_id: str | None = Field(
        default=None,
        description="The ID of the function.",
    )
    description: str | None = Field(
        default=None,
        description="The description of the function schedule.",
        min_length=1,
        max_length=500,
    )
    data: dict[str, str] | None = Field(
        default=None,
        description="nput data to the function.",
    )
    authentication: FunctionAuthenticationField | None = Field(
        default=None, description="Credentials required for the authentication."
    )

    @field_validator("cron_expression")
    @classmethod
    def validate_cron_expression(cls, v: str) -> str:
        """Validate corn expression"""
        if not croniter.is_valid(v):
            raise ValueError(f"{v} is a invalid cron expression.")
        return v
