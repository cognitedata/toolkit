from pydantic import Field

from .base import ToolkitResource


class TransformationScheduleYAML(ToolkitResource):
    external_id: str = Field(description="External ID of the scheduled transformation.")
    interval: str = Field(description="Cron expression describes when the job should run.")
    is_paused: bool | None = Field(default=None, description="If true, the transformation is not scheduled.")
