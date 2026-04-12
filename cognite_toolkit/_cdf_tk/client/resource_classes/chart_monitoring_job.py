from typing import Annotated, Any, Literal

from pydantic import Field, StrictStr, StringConstraints

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
    UpdatableRequestResource,
)
from cognite_toolkit._cdf_tk.client._types import Metadata
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, NodeUntypedId
from cognite_toolkit._cdf_tk.constants import MISSING_NONCE


class ChartMonitoringJobModel(BaseModelObject, extra="allow"):
    timeseries_id: int | None = None
    timeseries_external_id: str | None = None
    timeseries_instance_id: NodeUntypedId | None = None
    granularity: str | None = None
    external_id: Literal["double_threshold"] = "double_threshold"
    lower_threshold: float | None = None
    upper_threshold: float | None = None


URL_REGEX = r"^https://([^.]+(\.[^.]+)*\.)?(fusion|local)[\w\-]*(\.[^.]+)*\.(cognite\.com/|cognitedata\.com/|cogniteapp\.com/|cogheim\.net/|cognite\.ai(:4200)?/).+$"

ConstrainedUrl = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=1000, pattern=URL_REGEX),
]


class AlertContext(BaseModelObject, extra="allow"):
    unsubscribe_url: ConstrainedUrl
    investigate_url: ConstrainedUrl | None = None
    edit_url: ConstrainedUrl | None = None


class ChartMonitoringJob(BaseModelObject, extra="allow"):
    external_id: str
    name: StrictStr
    channel_id: int
    model: ChartMonitoringJobModel
    source: str | None = None
    source_id: str | None = None
    metadata: Metadata | None = None
    alert_context: AlertContext | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class ChartMonitoringJobRequest(ChartMonitoringJob, UpdatableRequestResource, extra="allow"):
    # This is the server generated identifier. It is not part of the request object,
    # but it is what the Chart.data object uses to reference the monitoring job, so we need to keep track of it in the request object.
    id: int | None = Field(None, exclude=True)
    interval: int | None = None
    overlap: int | None = None
    nonce: str

    def as_update(self, mode: Literal["patch", "replace"]) -> dict[str, Any]:
        return self.model_dump(
            exclude={"nonce", "source", "source_id", "metadata", "alert_context", "interval", "overlap"},
            exclude_none=True,
            by_alias=True,
        )


class ChartMonitoringJobResponse(ChartMonitoringJob, ResponseResource[ChartMonitoringJobRequest], extra="allow"):
    id: int
    interval: int
    overlap: int
    user_identifier: str | None = None

    @classmethod
    def request_cls(cls) -> type[ChartMonitoringJobRequest]:
        return ChartMonitoringJobRequest

    def as_request_resource(self) -> ChartMonitoringJobRequest:
        dump = self.dump()
        dump["nonce"] = MISSING_NONCE
        dump["id"] = self.id
        return ChartMonitoringJobRequest.model_validate(dump, extra="allow")
