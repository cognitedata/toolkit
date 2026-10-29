from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
    UpdatableRequestResource,
)
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId


class Domain(BaseModelObject):
    """Represents a domain in CDF."""

    external_id: str
    name: str
    description: str | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class DomainRequest(Domain, UpdatableRequestResource):
    """Request resource for creating/updating domains."""

    pass


class DomainResponse(Domain, ResponseResource[DomainRequest]):
    """Response resource for domains."""

    @classmethod
    def request_cls(cls) -> type[DomainRequest]:
        return DomainRequest
