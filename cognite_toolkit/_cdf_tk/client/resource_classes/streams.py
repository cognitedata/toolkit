from typing import Literal

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, RequestResource, ResponseResource
from cognite_toolkit._cdf_tk.constants import StreamTemplateName

from .identifiers import ExternalId


class Stream(BaseModelObject):
    external_id: str


class StreamRequest(Stream, RequestResource):
    """Stream request resource class."""

    settings: dict[Literal["template"], dict[Literal["name"], StreamTemplateName]]

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class LifecycleObject(BaseModelObject):
    """Lifecycle object."""

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


class StreamResponse(Stream, ResponseResource[StreamRequest]):
    """Stream response resource class."""

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
