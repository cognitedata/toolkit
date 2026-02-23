from typing import Literal, TypeAlias

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, RequestResource, ResponseResource
from cognite_toolkit._cdf_tk.client.resource_classes.charts_data import ChartData
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExternalId

Visibility: TypeAlias = Literal["PUBLIC", "PRIVATE"]


class Chart(BaseModelObject):
    external_id: str
    visibility: Visibility
    data: ChartData

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class ChartRequest(Chart, RequestResource): ...


class ChartResponse(Chart, ResponseResource[ChartRequest]):
    created_time: int
    last_updated_time: int
    owner_id: str

    def as_request_resource(self) -> ChartRequest:
        return ChartRequest.model_validate(self.dump(), extra="ignore")
