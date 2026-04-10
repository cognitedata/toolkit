from typing import Any, Literal

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
    UpdatableRequestResource,
)
from cognite_toolkit._cdf_tk.client._types import Metadata
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, NodeUntypedId


class ChartMonitoringJobModel(BaseModelObject, extra="allow"):
    timeseries_id: int | None = None
    timeseries_external_id: str | None = None
    timeseries_instance_id: NodeUntypedId | None = None
    granularity: str | None = None
    external_id: Literal["double_threshold"] = "double_threshold"
    lower_threshold: float | None = None
    upper_threshold: float | None = None


class AlertContext(BaseModelObject, extra="allow"):
    unsubscribe_url: str | None = None
    investigate_url: str | None = None
    edit_url: str | None = None


class ChartMonitoringJob(BaseModelObject, extra="allow"):
    source: str | None = None
    source_id: str | None = None


class ChartMonitoringJobSubscriber(BaseModelObject, extra="allow"):
    user_identifier: str | None = None
    email: str | None = None


class ChartMonitoringJobRequest(ChartMonitoringJob, UpdatableRequestResource, extra="allow"):
    monitoring_task_name: str | None = None
    folder_id: int | None = None
    evaluate_every: int | None = None
    activation_interval: str | None = None
    nonce: str | None = None
    threshold: float | None = None
    subscribers: list[ChartMonitoringJobSubscriber] | None = None
    current_url: str | None = None
    timeseries_id: int | None = None
    timeseries_external_id: str | None = None
    timeseries_instance_id: NodeUntypedId | None = None

    def as_id(self) -> ExternalId:
        raise ValueError(
            "ChartMonitoringJob does not have an external ID field. Use internal ID for retrieval and deletion."
        )

    def as_update(self, mode: Literal["patch", "replace"]) -> dict[str, Any]:
        raise NotImplementedError()


class ChartMonitoringJobResponse(ChartMonitoringJob, ResponseResource[ChartMonitoringJobRequest], extra="allow"):
    id: int | None = None
    name: str | None = None
    external_id: str | None = None
    channel_id: int | None = None
    interval: int | None = None
    overlap: int | None = None
    model: ChartMonitoringJobModel | None = None
    metadata: Metadata | None = None
    user_identifier: str | None = None
    status: str | None = None
    alert_context: AlertContext | None = None

    @classmethod
    def request_cls(cls) -> type[ChartMonitoringJobRequest]:
        return ChartMonitoringJobRequest

    def as_request_resource(self) -> ChartMonitoringJobRequest:
        raise NotImplementedError("Cannot be converted to ChartMonitoringJobRequest")
