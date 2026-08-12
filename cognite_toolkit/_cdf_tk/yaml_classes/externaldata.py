from pydantic import Field, SecretStr, field_serializer

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId

from .base import BaseModelResource, ToolkitResource


class OneLakeCredentialsYAML(BaseModelResource):
    """Azure credentials for OneLake access in toolkit YAML."""

    client_id: str = Field(description="Azure application (client) ID.")
    tenant_id: str = Field(description="Azure tenant (directory) ID.")
    client_secret: SecretStr | None = Field(default=None, description="Azure client secret.")

    @field_serializer("client_secret", when_used="json")
    def dump_client_secret(self, value: SecretStr | None) -> str | None:
        if value is None:
            return None
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


class ExternalDataSourceYAML(ToolkitResource):
    """Toolkit YAML representation of a Fabric OneLake external data source."""

    external_id: str = Field(description="The external ID provided by the client.")
    name: str | None = Field(default=None, description="Human-readable name for the external data source.")
    data_set_external_id: str | None = Field(
        default=None,
        description="External ID of the data set that owns this external data source.",
    )
    settings: OneLakeSettingsYAML = Field(description="OneLake connection settings.")

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)
