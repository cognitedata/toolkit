from typing import Literal

from cognite_toolkit._cdf_tk.constants import StreamTemplateName
from cognite_toolkit._cdf_tk.protocols import (
    ResourceRequestListProtocol,
    ResourceResponseListProtocol,
)

from .base import BaseModelObject, BaseResourceList, RequestResource, ResponseResource


class StreamRequest(RequestResource):
    """Stream request resource class."""

    external_id: str
    settings: dict[Literal["template"], dict[Literal["name"], StreamTemplateName]]

    def as_id(self) -> str:
        return self.external_id


class StreamRequestList(BaseResourceList[StreamRequest], ResourceRequestListProtocol):
    """List of Stream request resources."""

    _RESOURCE = StreamRequest


class LifecycleObject(BaseModelObject):
    """Lifecycle object."""

    hot_phase_duration: str | None = None
    data_deleted_after: str | None = None
    retained_after_soft_delete: str


class ResourceUsage(BaseModelObject):
    """Resource quota with provisioned and consumed values."""

    provisioned: int
    consumed: int | None = None


class LimitsObject(BaseModelObject):
    """Limits object."""

    max_records_total: ResourceUsage
    max_giga_bytes_total: ResourceUsage
    max_filtering_interval: str | None = None


class StreamSettings(BaseModelObject):
    """Stream settings object."""

    lifecycle: LifecycleObject
    limits: LimitsObject


class StreamResponse(ResponseResource["StreamRequest"]):
    """Stream response resource class."""

    external_id: str
    created_time: int
    created_from_template: StreamTemplateName
    type: Literal["Mutable", "Immutable"]
    settings: StreamSettings | None = None

    def as_request_resource(self) -> StreamRequest:
        return StreamRequest.model_validate(
            {
                "externalId": self.external_id,
                "settings": {"template": {"name": self.created_from_template}},
            }
        )

    def as_write(self) -> StreamRequest:
        return StreamRequest.model_validate(
            {
                "externalId": self.external_id,
                "settings": {"template": {"name": self.created_from_template}},
            }
        )


class StreamResponseList(BaseResourceList[StreamResponse], ResourceResponseListProtocol):
    """List of Stream response resources."""

    _RESOURCE = StreamResponse

    def as_write(self) -> StreamRequestList:
        return StreamRequestList([item.as_write() for item in self.data])
