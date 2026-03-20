from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    RequestResource,
    ResponseResource,
)
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId


class RuleSet(BaseModelObject):
    """Represents a rule set in CDF."""

    external_id: str
    name: str

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class RuleSetRequest(RuleSet, RequestResource):
    """Request resource for creating rule sets."""

    pass


class RuleSetResponse(RuleSet, ResponseResource[RuleSetRequest]):
    """Response resource for rule sets."""

    created_time: int

    @classmethod
    def request_cls(cls) -> type[RuleSetRequest]:
        return RuleSetRequest
