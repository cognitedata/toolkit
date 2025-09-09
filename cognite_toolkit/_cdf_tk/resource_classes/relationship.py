from typing import Literal

from pydantic import Field

from .base import BaseModelResource, ToolkitResource


class LabelRefYAML(BaseModelResource):
    externalId: str = Field(
        description="The external ID of the label.",
        max_length=255,
    )


class RelationshipYAML(ToolkitResource):
    external_id: str = Field(
        description="The external ID provided by the client.",
        max_length=255,
    )
    source_external_id: str = Field(
        description="The external ID of the resource that is the relationship source.",
    )
    source_type: Literal["asset", "timeSeries", "file", "event", "sequence"] = Field(
        description="The resource type of the relationship source. Must be one of the specified values."
    )
    target_external_id: str = Field(
        description="The external ID of the resource that is the relationship target.",
    )
    target_type: Literal["asset", "timeSeries", "file", "event", "sequence"] = Field(
        description="The resource type of the relationship target. Must be one of the specified values."
    )
    start_time: int | None = Field(
        None,
        description="The time, in milliseconds since Jan. 1, 1970, when the relationship became active. If there is no startTime, the relationship is active from the beginning of time until endTime.",
        ge=0,
    )
    end_time: int | None = Field(
        None,
        description="The time, in milliseconds since Jan. 1, 1970, when the relationship became inactive. If there is no endTime, the relationship is active until the end of time.",
        ge=0,
    )
    confidence: float | None = Field(
        None, description="A number between 0 and 1 that indicates the confidence in the relationship.", ge=0, le=1
    )
    data_set_external_id: str | None = Field(
        None, description="The external ID of the data set that the relationship belongs to.", max_length=255
    )
    labels: list[LabelRefYAML] | None = Field(
        None, description="A list of labels that the relationship belongs to.", max_length=10
    )
