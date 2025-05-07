from pydantic import Field

from .base import ToolkitResource


class EventYAML(ToolkitResource):
    external_id: str = Field(description="External ID provided by the client.", max_length=255)
    data_set_external_id: str | None = Field(None, description="The external id of the dataset this event belongs to.")
    start_time: int | None = Field(None, description="Epoch start time of the event", ge=0)
    end_time: int | None = Field(None, description="Epoch end time of the event", ge=0)
    type: str | None = Field(None, description="Type of the event, e.g. 'failure'.", max_length=64)
    subtype: str | None = Field(None, description="SubType of the event, e.g. 'electrical'.", max_length=64)
    description: str | None = Field(None, description="The description of the asset.", max_length=500)
    metadata: dict[str, str] | None = Field(None, description="Custom, application-specific metadata.", max_length=256)
    asset_external_ids: list[str] | None = Field(
        None, description="Asset IDs of equipment that this event relates to.", max_length=10000
    )
    source: str | None = Field(None, description="The source of this event.", max_length=128)
