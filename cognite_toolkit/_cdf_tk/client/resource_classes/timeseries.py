from typing import Any, ClassVar, Literal

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
    UpdatableRequestResource,
)

from .identifiers import ExternalId, InternalOrExternalId
from .instance_api import NodeReference


class TimeSeries(BaseModelObject):
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


class TimeSeriesRequest(TimeSeries, UpdatableRequestResource):
    container_fields: ClassVar[frozenset[str]] = frozenset({"metadata", "security_categories"})
    non_nullable_fields: ClassVar[frozenset[str]] = frozenset({"is_step"})

    def as_update(self, mode: Literal["patch", "replace"]) -> dict[str, Any]:
        dumped = super().as_update(mode)
        update = dumped["update"] if "update" in dumped else dumped
        # isString is immutable in CDF, so we remove it from update payloads
        update.pop("isString", None)
        return dumped


class TimeSeriesResponse(TimeSeries, ResponseResource[TimeSeriesRequest]):
    id: int
    instance_id: NodeReference | None = None
    type: str
    created_time: int
    last_updated_time: int

    def as_request_resource(self) -> TimeSeriesRequest:
        return TimeSeriesRequest.model_validate(self.dump(), extra="ignore")
