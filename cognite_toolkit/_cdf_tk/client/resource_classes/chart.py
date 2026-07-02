from collections.abc import Set
from typing import Literal, TypeAlias, get_args

from pydantic import Field

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    RequestResource,
    ResponseResource,
)
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.chart_monitoring_job import (
    ChartMonitoringJobRequest,
    ChartMonitoringJobResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.chart_scheduled_calculation import (
    ChartScheduledCalculationRequest,
    ChartScheduledCalculationResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.charts_data import ChartData

Visibility: TypeAlias = Literal["PUBLIC", "PRIVATE"]
BackendService: TypeAlias = Literal["monitoring_jobs", "scheduled_calculations"]
BACKEND_SERVICES: Set[BackendService] = frozenset(get_args(BackendService))


class Chart(BaseModelObject, extra="allow"):
    external_id: str
    visibility: Visibility
    data: ChartData

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class ChartRequest(Chart, RequestResource, extra="allow"):
    monitoring_jobs: list[ChartMonitoringJobRequest] | None = Field(None, exclude=True)
    scheduled_calculations: list[ChartScheduledCalculationRequest] | None = Field(None, exclude=True)


class ChartResponse(Chart, ResponseResource[ChartRequest], extra="allow"):
    monitoring_jobs: list[ChartMonitoringJobResponse] | None = Field(None, exclude=True)
    scheduled_calculations: list[ChartScheduledCalculationResponse] | None = Field(None, exclude=True)
    created_time: int
    last_updated_time: int
    owner_id: str

    @classmethod
    def request_cls(cls) -> type[ChartRequest]:
        return ChartRequest

    def as_request_resource(self, include: Set[BackendService] = BACKEND_SERVICES) -> ChartRequest:
        dumped = self.model_dump(
            mode="json",
            by_alias=True,
            exclude_unset=True,
            exclude={
                "created_time",
                "last_updated_time",
                "owner_id",
            },
        )
        chart_request = ChartRequest.model_validate(dumped, extra="allow", by_alias=True)
        if self.monitoring_jobs is not None and "monitoring_jobs" in include:
            chart_request.monitoring_jobs = [job.as_request_resource() for job in self.monitoring_jobs]
        if self.scheduled_calculations is not None and "scheduled_calculations" in include:
            chart_request.scheduled_calculations = [calc.as_request_resource() for calc in self.scheduled_calculations]
        return chart_request
