from typing import Any, Literal

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
    UpdatableRequestResource,
)
from cognite_toolkit._cdf_tk.client._types import Metadata
from cognite_toolkit._cdf_tk.client.identifiers import NodeUntypedId


class ChartMonitoringJobModel(BaseModelObject, extra="allow"):
    timeseries_id: int | None = None
    timeseries_external_id: str | None = None
    timeseries_instance_id: NodeUntypedId | None = None
    granularity: str | None = None
    external_id: str | None = "double_threshold"
    lower_threshold: float | None = None
    upper_threshold: float | None = None


class AlertContext(BaseModelObject, extra="allow"):
    unsubscribe_url: str | None = None
    investigate_url: str | None = None
    edit_url: str | None = None


class ChartMonitoringJob(BaseModelObject, extra="allow"):
    id: int
    name: str | None = None
    external_id: str | None = None
    channel_id: int | None = None
    interval: int | None = None
    overlap: int | None = None
    model: ChartMonitoringJobModel | None = None
    source: str | None = None
    source_id: str | None = None
    metadata: Metadata | None = None
    user_identifier: str | None = None
    status: str | None = None
    alert_context: AlertContext | None = None


class ChartMonitoringJobRequest(ChartMonitoringJob, UpdatableRequestResource, extra="allow"):
    def as_update(self, mode: Literal["patch", "replace"]) -> dict[str, Any]:
        return self.dump()


class ChartMonitoringJobResponse(ChartMonitoringJob, ResponseResource[ChartMonitoringJobRequest], extra="allow"):
    @classmethod
    def request_cls(cls) -> type[ChartMonitoringJobRequest]:
        return ChartMonitoringJobRequest
