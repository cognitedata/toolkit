import builtins
from typing import Any, ClassVar, Literal

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
    UpdatableRequestResource,
)
from cognite_toolkit._cdf_tk.client._types import Metadata
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, InternalOrExternalId, NodeReferenceUntyped


class TimeSeries(BaseModelObject):
    external_id: str | None = None
    name: str | None = None
    is_string: bool = False
    metadata: Metadata | None = None
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
    instance_id: NodeReferenceUntyped | None = None
    pending_instance_id: NodeReferenceUntyped | None = None
    type: str
    created_time: int
    last_updated_time: int

    @classmethod
    def request_cls(cls) -> builtins.type[TimeSeriesRequest]:
        return TimeSeriesRequest
