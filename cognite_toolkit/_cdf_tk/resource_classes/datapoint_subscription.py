import sys

from pydantic import Field, JsonValue, model_validator

from cognite_toolkit._cdf_tk.constants import SPACE_FORMAT_PATTERN

from .base import BaseModelResource, ToolkitResource

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

_SUBSCRIPTION_MAX_TOTAL_TIMESERIES = 10_000


class InstanceId(BaseModelResource):
    space: str = Field(
        description="The Space identifier (id).",
        min_length=1,
        max_length=43,
        pattern=SPACE_FORMAT_PATTERN,
    )
    external_id: str = Field(
        description="The external ID of the instance.",
        max_length=255,
    )


class DatapointSubscriptionYAML(ToolkitResource):
    external_id: str = Field(
        description="Externally provided ID for the subscription. Must be unique.",
        max_length=255,
    )
    name: str | None = Field(
        default=None,
        description="Human readable name of the subscription.",
    )
    description: str | None = Field(default=None, description="A description of the subscription.")
    data_set_external_id: str | None = Field(
        default=None,
        description="The dataSet Id for the item.",
    )
    partition_count: int | None = Field(
        default=None,
        ge=1,
        le=100,
        description="The maximum effective parallelism of this subscription (the number of clients that can read from it concurrently) will be limited to this number, but a higher partition count will cause a higher time overhead.",
    )
    time_series_ids: list[str] | None = Field(
        None,
        description="List of (external) ids of time series that this subscription will listen to. Not compatible with filter.",
    )
    instance_ids: list[InstanceId] | None = Field(
        None,
        description="List of instance ids of time series that this subscription will listen to. Not compatible with filter.",
    )

    filter: dict[str, JsonValue] | None = Field(
        default=None,
        description="A filter Domain Specific Language (DSL) used to create advanced filter queries. Not compatible with time_series_ids or instance_ids.",
    )

    @model_validator(mode="after")
    def check_subscription_targets(self) -> Self:
        time_series_ids = self.time_series_ids or []
        instance_ids = self.instance_ids or []

        total = len(time_series_ids) + len(instance_ids)
        if total > _SUBSCRIPTION_MAX_TOTAL_TIMESERIES:
            raise ValueError(
                f"The total number of time_series_ids and instance_ids cannot exceed {_SUBSCRIPTION_MAX_TOTAL_TIMESERIES}."
            )

        if self.filter is not None and total > 0:
            raise ValueError("Cannot set both filter and time_series_ids/instance_ids.")
        return self
