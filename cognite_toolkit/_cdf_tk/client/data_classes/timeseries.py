from typing import ClassVar

from cognite_toolkit._cdf_tk.client.data_classes.base import RequestUpdateable, ResponseResource

from .identifiers import ExternalId, InternalOrExternalId
from .instance_api import NodeReference


class TimeSeriesRequest(RequestUpdateable):
    container_fields: ClassVar[frozenset[str]] = frozenset({"metadata", "security_categories"})
    non_nullable_fields: ClassVar[frozenset[str]] = frozenset({"is_step"})
    external_id: str | None = None
    name: str | None = None
    is_string: bool = False
    metadata: dict[str, str] | None = None
    unit: str | None = None
    unit_external_id: str | None = None
    asset_id: int | None = None
    is_step: bool = False
    description: str | None = None
    security_categories: list[int] | None = None
    data_set_id: int | None = None

    def as_id(self) -> InternalOrExternalId:
        if self.external_id is None:
            raise ValueError("Cannot convert TimeSeriesRequest to ExternalId when external_id is None")
        return ExternalId(external_id=self.external_id)


class TimeSeriesResponse(ResponseResource[TimeSeriesRequest]):
    id: int
    instance_id: NodeReference | None = None
    external_id: str | None = None
    name: str | None = None
    is_string: bool
    metadata: dict[str, str] | None = None
    unit: str | None = None
    type: str
    unit_external_id: str | None = None
    asset_id: int | None = None
    is_step: bool
    description: str | None = None
    security_categories: list[int] | None = None
    data_set_id: int | None = None
    created_time: int
    last_updated_time: int

    def as_request_resource(self) -> TimeSeriesRequest:
        return TimeSeriesRequest.model_validate(self.dump())
