from typing import Literal, TypeAlias

from pydantic import Field

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, RequestResource, ResponseResource
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.charts_data import (
    ChartData,
    MonitoringUpdate,
    ScheduledCalculationUpdate,
)

Visibility: TypeAlias = Literal["PUBLIC", "PRIVATE"]


class Chart(BaseModelObject, extra="allow"):
    external_id: str
    visibility: Visibility
    data: ChartData
    # These are not part of the Chart frontend objects, instead they are part two backend services
    # for monitoring and scheduled calculations, respectively.
    monitoring_updates: list[MonitoringUpdate] | None = Field(None, exclude=True)
    scheduled_calculation_updates: list[ScheduledCalculationUpdate] | None = Field(None, exclude=True)

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class ChartRequest(Chart, RequestResource, extra="allow"): ...


class ChartResponse(Chart, ResponseResource[ChartRequest], extra="allow"):
    created_time: int
    last_updated_time: int
    owner_id: str

    @classmethod
    def request_cls(cls) -> type[ChartRequest]:
        return ChartRequest
