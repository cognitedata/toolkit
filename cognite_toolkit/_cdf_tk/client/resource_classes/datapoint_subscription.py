from typing import Any, ClassVar, Literal

from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
    UpdatableRequestResource,
)

from .identifiers import ExternalId
from .instance_api import NodeReference


class DatapointSubscription(BaseModelObject):
    external_id: str | None = None
    name: str | None = None
    description: str | None = None
    data_set_id: int | None = None
    time_series_ids: list[str] | None = None
    instance_ids: list[NodeReference] | None = None
    filter: JsonValue | None = None


class DatapointSubscriptionRequest(DatapointSubscription, UpdatableRequestResource):
    container_fields: ClassVar[frozenset[str]] = frozenset({"time_series_ids", "instance_ids"})
    partition_count: int | None = None

    def as_id(self) -> ExternalId:
        if self.external_id is None:
            raise ValueError("Cannot convert DatapointSubscriptionRequest to ExternalId when external_id is None")
        return ExternalId(external_id=self.external_id)

    def as_update(self, mode: Literal["patch", "replace"]) -> dict[str, Any]:
        dumped = super().as_update(mode)
        update = dumped["update"] if "update" in dumped else dumped
        # partitionCount is immutable in CDF, so we remove it from update payloads
        update.pop("partitionCount", None)
        return dumped


class DatapointSubscriptionResponse(DatapointSubscription, ResponseResource[DatapointSubscriptionRequest]):
    partition_count: int
    time_series_count: int
    created_time: int
    last_updated_time: int

    def as_request_resource(self) -> DatapointSubscriptionRequest:
        return DatapointSubscriptionRequest.model_validate(self.dump(), extra="ignore")
