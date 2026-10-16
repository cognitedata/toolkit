from typing import Annotated, Any, Literal

from pydantic import Field, SecretStr, ValidatorFunctionWrapHandler, WrapValidator, field_serializer

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId

from .base import BaseModelResource, ToolkitResource


class OneLakeCredentialsYAML(BaseModelResource):
    """Azure credentials for OneLake access in toolkit YAML."""

    client_id: str = Field(description="Azure application (client) ID.")
    tenant_id: str = Field(description="Azure tenant (directory) ID.")
    client_secret: SecretStr = Field(description="Azure client secret. Required on every write.")

    @field_serializer("client_secret", when_used="json")
    def dump_client_secret(self, value: SecretStr) -> str:
        return value.get_secret_value()


class OneLakeLocationDescriptionYAML(BaseModelResource):
    """Fabric workspace and lakehouse identifiers in toolkit YAML."""

    workspace_id: str = Field(description="Fabric workspace GUID (required).")
    container_id: str = Field(description="Fabric lakehouse GUID (required).")


class OneLakeSettingsYAML(BaseModelResource):
    """OneLake connection settings in toolkit YAML."""

    credentials: OneLakeCredentialsYAML = Field(description="Azure credentials for OneLake access.")
    location_description: OneLakeLocationDescriptionYAML = Field(
        description="Fabric workspace and lakehouse identifiers."
    )


class SnowflakeCredentialsYAML(BaseModelResource):
    """Snowflake credentials in toolkit YAML."""

    account_identifier: str = Field(description="Snowflake account identifier.")
    user_name: str = Field(description="Snowflake user name.")
    role_name: str = Field(description="Snowflake role used to connect.")


class SnowflakeLocationDescriptionYAML(BaseModelResource):
    """Snowflake location in toolkit YAML."""

    warehouse_name: str = Field(description="Snowflake warehouse used by the external data source.")


class SnowflakeSettingsYAML(BaseModelResource):
    """Snowflake connection settings in toolkit YAML."""

    credentials: SnowflakeCredentialsYAML = Field(description="Snowflake key-pair authentication settings.")
    location_description: SnowflakeLocationDescriptionYAML = Field(description="Snowflake warehouse settings.")


def _default_one_lake_format(value: Any, handler: ValidatorFunctionWrapHandler) -> Any:
    """Treat a missing format as OneLake without marking it as set by the user."""
    injected = False
    if isinstance(value, dict) and value.get("format") is None:
        value = {**value, "format": "one_lake"}
        injected = True
    loaded = handler(value)
    if injected:
        loaded.__pydantic_fields_set__.discard("format")
    return loaded


class ExternalDataSource(ToolkitResource):
    """Shared toolkit YAML fields for an external data source."""

    external_id: str = Field(description="The external ID provided by the client.")
    name: str | None = Field(default=None, description="Human-readable name for the external data source.")
    data_set_external_id: str | None = Field(
        default=None,
        description="External ID of the data set that owns this external data source.",
    )

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class OneLakeExternalDataSourceYAML(ExternalDataSource):
    """Toolkit YAML for a OneLake external data source."""

    format: Literal["one_lake"] = Field(default="one_lake", description="External data source format.")
    settings: OneLakeSettingsYAML = Field(description="OneLake connection settings.")


class SnowflakeExternalDataSourceYAML(ExternalDataSource):
    """Toolkit YAML for a Snowflake external data source."""

    format: Literal["snowflake"] = Field(default="snowflake", description="External data source format.")
    settings: SnowflakeSettingsYAML = Field(description="Snowflake connection settings.")
    expiry_time: int = Field(description="Expiry time in epoch milliseconds for the Snowflake RSA key pair.")


ExternalDataSourceYAML = Annotated[
    Annotated[
        OneLakeExternalDataSourceYAML | SnowflakeExternalDataSourceYAML,
        Field(discriminator="format"),
    ],
    WrapValidator(_default_one_lake_format),
]
