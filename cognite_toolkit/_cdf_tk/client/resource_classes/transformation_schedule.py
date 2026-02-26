from typing import ClassVar

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
    UpdatableRequestResource,
)
from cognite_toolkit._cdf_tk.client.identifiers.identifiers import ExternalId


class TransformationSchedule(BaseModelObject):
    external_id: str
    interval: str

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class TransformationScheduleRequest(TransformationSchedule, UpdatableRequestResource):
    container_fields: ClassVar[frozenset[str]] = frozenset()
    non_nullable_fields: ClassVar[frozenset[str]] = frozenset({"interval"})
    is_paused: bool | None = None


class TransformationScheduleResponse(TransformationSchedule, ResponseResource[TransformationScheduleRequest]):
    id: int
    created_time: int
    last_updated_time: int
    is_paused: bool

    @classmethod
    def request_cls(cls) -> type[TransformationScheduleRequest]:
        return TransformationScheduleRequest
