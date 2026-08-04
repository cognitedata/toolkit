from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    RequestResource,
    ResponseResource,
)
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId


class OneLakeCredentialsRead(BaseModelObject):
    client_id: str
    tenant_id: str


class OneLakeCredentialsWrite(BaseModelObject):
    client_id: str
    tenant_id: str
    client_secret: str | None = None


class OneLakeLocationDescription(BaseModelObject):
    workspace_name: str
    container_name: str


class OneLakeSettingsRead(BaseModelObject):
    credentials: OneLakeCredentialsRead | None = None
    location_description: OneLakeLocationDescription | None = None


class OneLakeSettingsWrite(BaseModelObject):
    credentials: OneLakeCredentialsWrite | None = None
    location_description: OneLakeLocationDescription | None = None


class ExternalDataSourceCore(BaseModelObject):
    external_id: str
    name: str | None = None
    data_set_id: int | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class ExternalDataSourceRequest(ExternalDataSourceCore, RequestResource):
    format: str = "one_lake"
    settings: OneLakeSettingsWrite | None = None


class ExternalDataSourceResponse(ExternalDataSourceCore, ResponseResource[ExternalDataSourceRequest]):
    format: str | None = None
    settings: OneLakeSettingsRead | None = None
    created_time: int
    last_updated_time: int

    @classmethod
    def request_cls(cls) -> type[ExternalDataSourceRequest]:
        return ExternalDataSourceRequest
