import builtins
from typing import Literal

from pydantic import ConfigDict

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, RequestResource, ResponseResource
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.feature_flags import Flags


class StreamsModelObject(BaseModelObject):
    model_config = ConfigDict(extra="allow")


# The StreamsModelObject is temporary until v0.8 is released.
# This flag is set here to remind us to remove the StreamsModelObject and use BaseModelObject
# directly in the Stream and StreamTemplate classes.
ModelObject: type = BaseModelObject if Flags.v08.is_enabled() else StreamsModelObject


class Stream(ModelObject):
    external_id: str

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class StreamTemplate(ModelObject):
    # The literal contains the officially support templates, in addition, we
    # allow any string to support potential custom templates that may be used by some customers.
    name: Literal["ImmutableTestStream", "BasicArchive", "BasicLiveData"] | str


class StreamRequestSettings(ModelObject):
    template: StreamTemplate


class StreamRequest(Stream, RequestResource):
    """Stream request resource class."""

    settings: StreamRequestSettings


class LifecycleObject(ModelObject):
    """Lifecycle object."""

    data_deleted_after: str | None = None
    retained_after_soft_delete: str


class ResourceUsage(ModelObject):
    """Resource quota with provisioned and consumed values."""

    provisioned: int | float
    consumed: int | float | None = None


class LimitsObject(ModelObject):
    """Limits object."""

    max_records_total: ResourceUsage
    max_giga_bytes_total: ResourceUsage
    max_filtering_interval: str | None = None


class StreamSettings(ModelObject):
    """Stream settings object."""

    lifecycle: LifecycleObject
    limits: LimitsObject


class StreamResponse(Stream, ResponseResource[StreamRequest]):
    """Stream response resource class."""

    created_time: int
    created_from_template: str
    type: Literal["Mutable", "Immutable"]
    settings: StreamSettings | None = None

    @classmethod
    def request_cls(cls) -> builtins.type[StreamRequest]:
        return StreamRequest

    def as_request_resource(self) -> StreamRequest:
        return StreamRequest.model_validate(
            {
                "externalId": self.external_id,
                "settings": {"template": {"name": self.created_from_template}},
            }
        )
