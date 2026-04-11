from datetime import datetime
from typing import Any, Literal

from pydantic import Field, JsonValue

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
    UpdatableRequestResource,
)
from cognite_toolkit._cdf_tk.client.identifiers import NodeUntypedId

SECOND_MS = 1000
MINUTE_MS = SECOND_MS * 60
HOUR_MS = MINUTE_MS * 60
DAY_MS = HOUR_MS * 24


class CalculationStep(BaseModelObject):
    op: str
    version: float
    inputs: list[JsonValue]
    raw: bool
    step: int
    params: dict[str, JsonValue] | None = None


class CalculationGraph(BaseModelObject):
    granularity: str
    steps: list[CalculationStep]


class CalculationTaskStatus(BaseModelObject):
    last_processed_timestamp_utc: datetime | None = None
    last_fire_time_utc: datetime | None = None
    last_scheduled_fire_time_utc: datetime | None = None
    last_run_request_id: str | None = None
    run_count: int | None = None
    last_run_successful: bool | None = None
    last_success_fire_time_utc: datetime | None = None
    last_success_scheduled_fire_time_utc: datetime | None = None
    last_success_run_request_id: str | None = None
    success_run_count: int | None = None
    last_failure_fire_time_utc: datetime | None = None
    last_failure_scheduled_fire_time_utc: datetime | None = None
    last_failure_run_request_id: str | None = None
    last_failure_message: str | None = None
    last_failure_code: int | None = None
    failure_run_count: int | None = None


class ChartScheduledCalculation(BaseModelObject):
    name: str | None = None
    external_id: str
    description: str | None = None
    period: int = Field(
        description="Schedule period in milliseconds. It determines the how often the calculation will run"
        + " and the time window for the input and output data."
        + " The minimum period and window is 5 minute and the maximum is 30 days.",
        ge=MINUTE_MS,
        le=30 * DAY_MS,
        multiple_of=MINUTE_MS,
    )
    offset: int | None = Field(
        default=None,
        description="Calculation window offset in milliseconds. For a given execution of the calculation task,"
        + " the calculation window is defined like this: [ start - window - offset ; start - offset ]",
        ge=0,
        multiple_of=SECOND_MS,
    )
    window_size: int | None = Field(
        default=None,
        description="Calculation window size in milliseconds. If not set, the window size will be equal to the period.",
    )
    target_timeseries_external_id: str | None = None
    targetTimeseriesInstanceId: NodeUntypedId | None = None
    graph: CalculationGraph


class ChartScheduledCalculationRequest(ChartScheduledCalculation, UpdatableRequestResource):
    nonce: str

    def as_update(self, mode: Literal["patch", "replace"]) -> dict[str, Any]:
        return self.model_dump(exclude={"nonce"}, exclude_none=True)


class ChartScheduledCalculationResponse(ChartScheduledCalculation, ResponseResource[ChartScheduledCalculationRequest]):
    created_time: int | None = None
    last_updated_time: int | None = None
    status: CalculationTaskStatus | None = None

    @classmethod
    def request_cls(cls) -> type[ChartScheduledCalculationRequest]:
        return ChartScheduledCalculationRequest

    def as_request_resource(self) -> ChartScheduledCalculationRequest:
        dump = self.model_dump(
            mode="python", by_alias=True, exclude_unset=True, exclude={"created_time", "last_updated_time", "status"}
        )
        dump["nonce"] = "<missing>"
        return ChartScheduledCalculationRequest.model_validate(dump, extra="allow")
