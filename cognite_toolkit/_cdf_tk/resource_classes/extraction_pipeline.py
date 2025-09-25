from typing import Literal

from pydantic import Field

from .base import BaseModelResource, ToolkitResource


class RawTable(BaseModelResource):
    db_name: str = Field(description="Database name", min_length=1)
    table_name: str = Field(description="Table name", min_length=1)


class Contact(BaseModelResource):
    name: str | None = Field(None, description="Contact name")
    email: str | None = Field(None, description="Contact email", min_length=1, max_length=254)
    role: str | None = Field(None, description="Contact role")
    send_notification: bool | None = Field(None, description="True, if contact receives email notifications")


class NotificationConfig(BaseModelResource):
    allowed_not_seen_range_in_minutes: int | None = Field(
        None,
        ge=0,
        description="Notifications configuration value. Time in minutes to pass without any Run. Null if extraction pipeline is not checked.",
    )


class ExtractionPipelineYAML(ToolkitResource):
    external_id: str = Field(
        description="External Id provided by client. Should be unique within the project.", min_length=1, max_length=255
    )
    name: str = Field(description="Name of the extraction pipeline.", min_length=1, max_length=140)
    description: str | None = Field(None, description="Description of the extraction pipeline.", max_length=500)
    data_set_external_id: str = Field(description="The external id of the dataset this extraction pipeline belongs to.")
    raw_tables: list[RawTable] | None = Field(None, description="Raw tables")
    schedule: Literal["On trigger", "Continuous"] | str | None = Field(
        None,
        description="Possible values: “On trigger”, “Continuous” or cron expression. If empty then null.",
    )
    contacts: list[Contact] | None = Field(None, description="Contacts list.")
    metadata: dict[str, str] | None = Field(
        None,
        description="Custom, application specific metadata. String key -> String value. Limits: Key are at most 128 bytes. Values are at most 10240 bytes. Up to 256 key-value pairs. Total size is at most 10240.",
    )
    source: str | None = Field(None, description="Source for Extraction Pipeline", max_length=255)
    documentation: str | None = Field(
        None, description="Documentation text field, supports Markdown for text formatting.", max_length=10000
    )
    notification_config: NotificationConfig | None = Field(
        None, description="Notification configuration for the extraction pipeline."
    )
    created_by: str | None = Field(
        None, description="Extraction Pipeline creator. Usually user email is expected here."
    )
