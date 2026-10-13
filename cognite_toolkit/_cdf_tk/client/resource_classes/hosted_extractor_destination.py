from typing import ClassVar

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
    UpdatableRequestResource,
)
from cognite_toolkit._cdf_tk.client._types import Metadata
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId


class Credentials(BaseModelObject):
    nonce: str


class HostedExtractorDestination(BaseModelObject):
    external_id: str
    name: str | None = None
    description: str | None = None
    metadata: Metadata | None = None
    target_data_set_id: int | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class HostedExtractorDestinationRequest(HostedExtractorDestination, UpdatableRequestResource):
    container_fields: ClassVar[frozenset[str]] = frozenset({"metadata"})
    credentials: Credentials | None = None


class HostedExtractorDestinationResponse(
    HostedExtractorDestination, ResponseResource[HostedExtractorDestinationRequest]
):
    session_id: int | None = None
    created_time: int
    last_updated_time: int

    @classmethod
    def request_cls(cls) -> type[HostedExtractorDestinationRequest]:
        return HostedExtractorDestinationRequest
