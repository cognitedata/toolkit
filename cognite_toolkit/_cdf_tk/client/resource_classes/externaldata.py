from typing import Annotated, Any, Literal

from pydantic import BeforeValidator, TypeAdapter

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


class ExternalDataSourceCore(BaseModelObject):
    """Shared fields for external data source request and response resources."""

    external_id: str
    name: str | None = None
    data_set_id: int | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class ExternalDataSourceRequest(ExternalDataSourceCore, RequestResource):
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


class UnknownExternalDataSourceRequest(ExternalDataSourceRequest):
    """Request resource for an external data source format not known to Toolkit."""

    format: str


class ExternalDataSourceResponse(ExternalDataSourceCore, ResponseResource[ExternalDataSourceRequest]):
    """Base response for an external data source. Use a format-specific subclass."""

    created_time: int
    last_updated_time: int


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


class UnknownExternalDataSourceResponse(ExternalDataSourceResponse):
    """Response resource for an external data source format not known to Toolkit."""

    format: str

    @classmethod
    def request_cls(cls) -> type[UnknownExternalDataSourceRequest]:
        return UnknownExternalDataSourceRequest

    def as_request_resource(self) -> UnknownExternalDataSourceRequest:
        # extra="allow" keeps format-specific fields such as settings, which Toolkit does not model.
        dumped = self.dump()
        dumped.pop("createdTime", None)
        dumped.pop("lastUpdatedTime", None)
        return UnknownExternalDataSourceRequest.model_validate(dumped, extra="allow")


_REQUEST_BY_FORMAT: dict[str, type[ExternalDataSourceRequest]] = {
    "one_lake": OneLakeExternalDataSourceRequest,
    "snowflake": SnowflakeExternalDataSourceRequest,
}
_RESPONSE_BY_FORMAT: dict[str, type[ExternalDataSourceResponse]] = {
    "one_lake": OneLakeExternalDataSourceResponse,
    "snowflake": SnowflakeExternalDataSourceResponse,
}


def _handle_request_union(value: Any) -> Any:
    # A missing format is OneLake, the only format before Snowflake was added.
    if isinstance(value, dict):
        source_format = value.get("format") or "one_lake"
        return _REQUEST_BY_FORMAT.get(source_format, UnknownExternalDataSourceRequest).model_validate(value)
    return value


def _handle_response_union(value: Any) -> Any:
    if isinstance(value, dict):
        source_format = value.get("format") or "one_lake"
        return _RESPONSE_BY_FORMAT.get(source_format, UnknownExternalDataSourceResponse).model_validate(value)
    return value


ExternalDataSourceRequestUnion = Annotated[
    OneLakeExternalDataSourceRequest | SnowflakeExternalDataSourceRequest | UnknownExternalDataSourceRequest,
    BeforeValidator(_handle_request_union),
]
ExternalDataSourceRequestAdapter: TypeAdapter[ExternalDataSourceRequestUnion] = TypeAdapter(
    ExternalDataSourceRequestUnion
)

ExternalDataSourceResponseUnion = Annotated[
    OneLakeExternalDataSourceResponse | SnowflakeExternalDataSourceResponse | UnknownExternalDataSourceResponse,
    BeforeValidator(_handle_response_union),
]
ExternalDataSourceResponseAdapter: TypeAdapter[ExternalDataSourceResponseUnion] = TypeAdapter(
    ExternalDataSourceResponseUnion
)


class ExternalDataSourceUsabilityResponse(BaseModelObject):
    """Response from the external data source usability check endpoint."""

    external_id: str
    usable_version: str | None = None


class RotateKeysRequest(BaseModelObject):
    """Request to rotate the RSA key pair for a Snowflake external data source."""

    external_id: str
    expiry_time: int


class RotatedKeysResponse(BaseModelObject):
    """New public key and expiry returned after rotating a Snowflake key pair."""

    external_id: str
    public_key: str
    expiry_time: int
