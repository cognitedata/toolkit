from typing import Any, Generic, Literal, TypeVar

from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    RequestResource,
    ResponseResource,
)
from cognite_toolkit._cdf_tk.client.identifiers.identifiers import ExternalId

from .data_modeling import NodeReference

# Unlike other resources, datapoints subscriptions have a separate update resource. This
# is because datapoint subscriptions need to handle add/removing operations for timeSeriesIds and
# instanceIds.

T_Value = TypeVar("T_Value")


class Set(BaseModelObject, Generic[T_Value]):
    set: T_Value


class SetNull(BaseModelObject):
    set_null: Literal[True] = True


class AddRemove(BaseModelObject, Generic[T_Value]):
    add: T_Value | None = None
    remove: T_Value | None = None


class DataPointSubscriptionUpdate(BaseModelObject):
    time_series_ids: AddRemove[list[str]] | Set[list[str]] | None = None
    instance_ids: AddRemove[list[NodeReference]] | Set[list[NodeReference]] | None = None
    name: Set[str] | SetNull | None = None
    description: Set[str] | SetNull | None = None
    data_set_id: Set[int] | SetNull | None = None
    filter: Set[JsonValue] | None = None


class DatapointSubscriptionUpdateRequest(RequestResource):
    external_id: str
    update: DataPointSubscriptionUpdate

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)

    def has_data(self) -> bool:
        """Check if the update request contains any data to update."""
        return self.update.model_dump(exclude_none=True) != {}


class DatapointSubscription(BaseModelObject):
    external_id: str
    name: str | None = None
    description: str | None = None
    data_set_id: int | None = None
    partition_count: int
    filter: JsonValue | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class DatapointSubscriptionRequest(DatapointSubscription, RequestResource):
    time_series_ids: list[str] | None = None
    instance_ids: list[NodeReference] | None = None

    def as_update(self) -> DatapointSubscriptionUpdateRequest:
        if self.time_series_ids is not None:
            raise NotImplementedError("Bug in Toolkit. Trying to update timeSeriesIds without an update object")
        if self.instance_ids is not None:
            raise NotImplementedError("Bug in Toolkit. Trying to update instanceIds without an update object")
        filter_arg: dict[str, Any] = {}
        if self.filter is not None:
            filter_arg["filter"] = Set(set=self.filter)
        return DatapointSubscriptionUpdateRequest(
            external_id=self.external_id,
            update=DataPointSubscriptionUpdate(
                name=SetNull() if self.name is None else Set(set=self.name),
                description=SetNull() if self.description is None else Set(set=self.description),
                data_set_id=SetNull() if self.data_set_id is None else Set(set=self.data_set_id),
                **filter_arg,
            ),
        )


class DatapointSubscriptionResponse(DatapointSubscription, ResponseResource[DatapointSubscriptionRequest]):
    time_series_count: int | None = None
    created_time: int
    last_updated_time: int

    @classmethod
    def request_cls(cls) -> type[DatapointSubscriptionRequest]:
        """Return the class of the corresponding request resource."""
        return DatapointSubscriptionRequest
