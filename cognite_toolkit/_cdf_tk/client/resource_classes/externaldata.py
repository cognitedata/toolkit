from abc import ABC, abstractmethod
from typing import Annotated, Any, Literal

from pydantic import BeforeValidator, Field, TypeAdapter

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


class SnowflakeCredentialsRead(BaseModelObject):
    """Credentials returned for a Snowflake external data source."""

    account_identifier: str
    user_name: str
    role_name: str
    public_key: str


class SnowflakeCredentialsWrite(BaseModelObject):
    """Credentials for creating a Snowflake external data source."""

    account_identifier: str
    user_name: str
    role_name: str


class SnowflakeLocationDescription(BaseModelObject):
    """Snowflake warehouse used by the external data source."""

    warehouse_name: str


class SnowflakeSettingsRead(BaseModelObject):
    """Snowflake connection settings returned from the API."""

    credentials: SnowflakeCredentialsRead
    location_description: SnowflakeLocationDescription


class SnowflakeSettingsWrite(BaseModelObject):
    """Snowflake connection settings for create requests."""

    credentials: SnowflakeCredentialsWrite
    location_description: SnowflakeLocationDescription


def _default_one_lake_format(value: Any) -> Any:
    """Treat a missing format as OneLake so existing payloads stay valid."""
    if isinstance(value, dict) and value.get("format") is None:
        return {**value, "format": "one_lake"}
    return value


class ExternalDataSourceCore(BaseModelObject):
    """Shared fields for external data source request and response resources."""

    external_id: str
    name: str | None = None
    data_set_id: int | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class ExternalDataSourceRequest(ExternalDataSourceCore, RequestResource, ABC):
    """Base request for an external data source. Use a format-specific subclass."""


class OneLakeExternalDataSourceRequest(ExternalDataSourceRequest):
    """Request resource for a OneLake external data source."""

    format: Literal["one_lake"] = "one_lake"
    settings: OneLakeSettingsWrite | None = None


class SnowflakeExternalDataSourceRequest(ExternalDataSourceRequest):
    """Request resource for a Snowflake external data source."""

    format: Literal["snowflake"] = "snowflake"
    settings: SnowflakeSettingsWrite
    expiry_time: int


ExternalDataSourceRequestUnion = Annotated[
    Annotated[
        OneLakeExternalDataSourceRequest | SnowflakeExternalDataSourceRequest,
        Field(discriminator="format"),
    ],
    BeforeValidator(_default_one_lake_format),
]
ExternalDataSourceRequestAdapter: TypeAdapter[ExternalDataSourceRequestUnion] = TypeAdapter(
    ExternalDataSourceRequestUnion
)


class ExternalDataSourceResponse(ExternalDataSourceCore, ResponseResource[ExternalDataSourceRequest], ABC):
    """Base response for an external data source. Use a format-specific subclass."""

    created_time: int
    last_updated_time: int

    @classmethod
    @abstractmethod
    def request_cls(cls) -> type[ExternalDataSourceRequest]:
        raise NotImplementedError


class OneLakeExternalDataSourceResponse(ExternalDataSourceResponse):
    """Response resource for a OneLake external data source."""

    format: Literal["one_lake"] = "one_lake"
    settings: OneLakeSettingsRead | None = None

    @classmethod
    def request_cls(cls) -> type[OneLakeExternalDataSourceRequest]:
        return OneLakeExternalDataSourceRequest


class SnowflakeExternalDataSourceResponse(ExternalDataSourceResponse):
    """Response resource for a Snowflake external data source."""

    format: Literal["snowflake"] = "snowflake"
    settings: SnowflakeSettingsRead
    expiry_time: int

    @classmethod
    def request_cls(cls) -> type[SnowflakeExternalDataSourceRequest]:
        return SnowflakeExternalDataSourceRequest


ExternalDataSourceResponseUnion = Annotated[
    Annotated[
        OneLakeExternalDataSourceResponse | SnowflakeExternalDataSourceResponse,
        Field(discriminator="format"),
    ],
    BeforeValidator(_default_one_lake_format),
]
ExternalDataSourceResponseAdapter: TypeAdapter[ExternalDataSourceResponseUnion] = TypeAdapter(
    ExternalDataSourceResponseUnion
)


class ExternalDataSourceUsabilityResponse(BaseModelObject):
    """Response from the external data source usability check endpoint."""

    external_id: str
    usable_version: str | None = None


class RotateKeysRequest(RequestResource):
    """Request to rotate the RSA key pair for a Snowflake external data source."""

    external_id: str
    expiry_time: int

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class RotatedKeysResponse(BaseModelObject):
    """New public key and expiry returned after rotating a Snowflake key pair."""

    external_id: str
    public_key: str
    expiry_time: int
