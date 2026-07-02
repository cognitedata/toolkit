from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, RequestResource, ResponseResource
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId


class App(BaseModelObject):
    external_id: str
    name: str
    description: str | None = None


class AppRequest(App, RequestResource):
    """Write resource for POST /apphosting/apps."""

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class AppResponse(App, ResponseResource[AppRequest]):
    """Response from GET/POST /apphosting/apps."""

    @classmethod
    def request_cls(cls) -> type[AppRequest]:
        return AppRequest
