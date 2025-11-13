import sys
from collections import UserList
from typing import Any, Literal

from cognite.client import CogniteClient

from cognite_toolkit._cdf_tk.constants import StreamTemplateName
from cognite_toolkit._cdf_tk.protocols import ResourceRequestListProtocol, ResourceResponseListProtocol

from .base import BaseModelObject, RequestResource, ResponseResource

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class StreamRequest(RequestResource):
    """Stream request resource class."""

    external_id: str
    settings: dict[Literal["template"], dict[Literal["name"], StreamTemplateName]]

    def as_id(self) -> str:
        return self.external_id


class StreamRequestList(UserList[StreamRequest], ResourceRequestListProtocol):
    """List of Stream request resources."""

    _RESOURCE = StreamRequest
    items: list[StreamRequest]

    def __init__(self, initlist: list[StreamRequest] | None = None, **_: Any) -> None:
        super().__init__(initlist or [])

    def dump(self, camel_case: bool = True) -> list[dict[str, Any]]:
        return [item.dump(camel_case) for item in self.items]

    @classmethod
    def load(cls, data: list[dict[str, Any]], cognite_client: CogniteClient | None = None) -> "StreamRequestList":
        items = [StreamRequest.model_validate(item) for item in data]
        return cls(items)


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
    created_from_template: StreamTemplateName | None = None
    type: Literal["Mutable", "Immutable"] | None = None
    settings: StreamSettings | None = None

    def as_request_resource(self) -> StreamRequest:
        template_name = self.created_from_template or "BasicArchive"
        return StreamRequest(
            external_id=self.external_id,
            settings={"template": {"name": template_name}},
        )

    def as_write(self) -> Self:
        return self


class StreamResponseList(UserList[StreamResponse], ResourceResponseListProtocol):
    """List of Stream response resources."""

    _RESOURCE = StreamResponse
    data: list[StreamResponse]

    def __init__(self, initlist: list[StreamResponse] | None = None, **_: Any) -> None:
        super().__init__(initlist or [])

    def dump(self, camel_case: bool = True) -> list[dict[str, Any]]:
        return [item.dump(camel_case) for item in self.data]

    @classmethod
    def load(cls, data: list[dict[str, Any]], cognite_client: CogniteClient | None = None) -> "StreamResponseList":
        items = [StreamResponse.model_validate(item) for item in data]
        return cls(items)

    def as_write(self) -> Self:
        return self
