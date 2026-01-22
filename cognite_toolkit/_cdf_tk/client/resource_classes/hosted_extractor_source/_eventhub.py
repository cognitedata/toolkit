from typing import Literal

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
)

from ._auth import BasicAuthenticationRequest, BasicAuthenticationResponse
from ._base import SourceRequestDefinition, SourceResponseDefinition


class EventHubSource(BaseModelObject):
    type: Literal["eventHub"] = "eventHub"
    host: str
    event_hub_name: str
    consumer_group: str | None = None


class EventHubSourceRequest(EventHubSource, SourceRequestDefinition):
    authentication: BasicAuthenticationRequest | None = None


class EventHubSourceResponse(
    SourceResponseDefinition,
    EventHubSource,
    ResponseResource[EventHubSourceRequest],
):
    authentication: BasicAuthenticationResponse | None = None

    def as_request_resource(self) -> EventHubSourceRequest:
        return EventHubSourceRequest.model_validate(self.dump(), extra="ignore")
