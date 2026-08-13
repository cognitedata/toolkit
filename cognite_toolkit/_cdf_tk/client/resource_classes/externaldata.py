from typing import Literal

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    RequestResource,
    ResponseResource,
)
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId


class OneLakeCredentialsRead(BaseModelObject):
    """Azure credentials returned for a OneLake external data source."""

    client_id: str
    tenant_id: str


class OneLakeCredentialsWrite(BaseModelObject):
    """Azure credentials for creating or updating a OneLake external data source."""

    client_id: str
    tenant_id: str
    client_secret: str | None = None


class OneLakeLocationDescription(BaseModelObject):
    """Fabric workspace and lakehouse identifiers for OneLake."""

    workspace_id: str
    container_id: str


class OneLakeSettingsRead(BaseModelObject):
    """OneLake connection settings returned from the API."""

    credentials: OneLakeCredentialsRead | None = None
    location_description: OneLakeLocationDescription | None = None


class OneLakeSettingsWrite(BaseModelObject):
    """OneLake connection settings for create/update requests."""

    credentials: OneLakeCredentialsWrite | None = None
    location_description: OneLakeLocationDescription | None = None


class ExternalDataSourceCore(BaseModelObject):
    """Shared fields for external data source request and response resources."""

    external_id: str
    name: str | None = None
    data_set_id: int | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class ExternalDataSourceRequest(ExternalDataSourceCore, RequestResource):
    """Request resource for creating or updating an external data source."""

    format: Literal["one_lake"] = "one_lake"
    settings: OneLakeSettingsWrite | None = None


class ExternalDataSourceResponse(ExternalDataSourceCore, ResponseResource[ExternalDataSourceRequest]):
    """Response resource for an external data source."""

    created_time: int
    last_updated_time: int
    format: str | None = None
    settings: OneLakeSettingsRead | None = None

    @classmethod
    def request_cls(cls) -> type[ExternalDataSourceRequest]:
        return ExternalDataSourceRequest


class ExternalDataSourceUsabilityResponse(BaseModelObject):
    """Response from the external data source usability check endpoint."""

    external_id: str
    usable_version: str | None = None
