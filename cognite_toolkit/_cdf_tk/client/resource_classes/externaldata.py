from typing import Annotated, Literal

from pydantic import Field

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
    client_secret: str


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


class SnowflakeCredentialsWrite(BaseModelObject):
    """Snowflake user and role. Cognite generates the RSA key pair used for key-pair authentication."""

    account_identifier: str
    user_name: str
    role_name: str


class SnowflakeCredentialsRead(SnowflakeCredentialsWrite):
    """Snowflake credentials returned from the API. Register the public key on the Snowflake user."""

    public_key: str


class SnowflakeLocationDescription(BaseModelObject):
    warehouse_name: str


class SnowflakeSettingsRead(BaseModelObject):
    credentials: SnowflakeCredentialsRead
    location_description: SnowflakeLocationDescription


class SnowflakeSettingsWrite(BaseModelObject):
    credentials: SnowflakeCredentialsWrite
    location_description: SnowflakeLocationDescription


class ExternalDataSourceCore(BaseModelObject):
    """Shared fields for external data source request and response resources."""

    external_id: str
    name: str | None = None
    data_set_id: int | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class ExternalDataSourceRequest(ExternalDataSourceCore, RequestResource):
    """Base request resource for creating an external data source."""

    format: str


class OneLakeExternalDataSourceRequest(ExternalDataSourceRequest):
    """Request resource for creating a Fabric OneLake external data source."""

    format: Literal["one_lake"] = "one_lake"
    settings: OneLakeSettingsWrite | None = None


class SnowflakeExternalDataSourceRequest(ExternalDataSourceRequest):
    """Request resource for creating a Snowflake external data source.

    ``expiry_time`` is the expiry of the RSA key pair Cognite generates for key-pair authentication.
    """

    format: Literal["snowflake"] = "snowflake"
    settings: SnowflakeSettingsWrite | None = None
    expiry_time: int


class ExternalDataSourceResponse(ExternalDataSourceCore, ResponseResource[ExternalDataSourceRequest]):
    """Base response resource for an external data source."""

    created_time: int
    last_updated_time: int
    format: str


class OneLakeExternalDataSourceResponse(ExternalDataSourceResponse):
    """Response resource for a Fabric OneLake external data source."""

    format: Literal["one_lake"] = "one_lake"
    settings: OneLakeSettingsRead | None = None

    @classmethod
    def request_cls(cls) -> type[OneLakeExternalDataSourceRequest]:
        return OneLakeExternalDataSourceRequest


class SnowflakeExternalDataSourceResponse(ExternalDataSourceResponse):
    """Response resource for a Snowflake external data source."""

    format: Literal["snowflake"] = "snowflake"
    settings: SnowflakeSettingsRead | None = None
    expiry_time: int | None = None

    @classmethod
    def request_cls(cls) -> type[SnowflakeExternalDataSourceRequest]:
        return SnowflakeExternalDataSourceRequest


ExternalDataSourceRequestUnion = Annotated[
    OneLakeExternalDataSourceRequest | SnowflakeExternalDataSourceRequest,
    Field(discriminator="format"),
]

ExternalDataSourceResponseUnion = Annotated[
    OneLakeExternalDataSourceResponse | SnowflakeExternalDataSourceResponse,
    Field(discriminator="format"),
]


class ExternalDataSourceUsabilityResponse(BaseModelObject):
    """Response from the external data source usability check endpoint."""

    external_id: str
    usable_version: str | None = None


class ExternalDataSourceRotateKeysRequest(BaseModelObject):
    """Generate a new RSA key pair for a Snowflake external data source, valid until ``expiry_time``."""

    external_id: str
    expiry_time: int


class ExternalDataSourceRotatedKeys(BaseModelObject):
    """The new public key, which must be registered on the Snowflake user."""

    external_id: str
    public_key: str
    expiry_time: int
