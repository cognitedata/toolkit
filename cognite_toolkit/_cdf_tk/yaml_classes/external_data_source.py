from pydantic import Field, SecretStr, field_serializer

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId

from .base import BaseModelResource, ToolkitResource


class OneLakeCredentialsYAML(BaseModelResource):
    client_id: str = Field(description="Azure application (client) ID.")
    tenant_id: str = Field(description="Azure tenant (directory) ID.")
    client_secret: SecretStr = Field(description="Azure client secret.")

    @field_serializer("client_secret", when_used="json")
    def dump_client_secret(self, value: SecretStr) -> str:
        return value.get_secret_value()


class OneLakeLocationDescriptionYAML(BaseModelResource):
    workspace_name: str = Field(description="Fabric workspace GUID or name.")
    container_name: str = Field(description="Fabric lakehouse GUID or name.")


class OneLakeSettingsYAML(BaseModelResource):
    credentials: OneLakeCredentialsYAML = Field(description="Azure credentials for OneLake access.")
    location_description: OneLakeLocationDescriptionYAML = Field(
        description="Fabric workspace and lakehouse identifiers."
    )


class ExternalDataSourceYAML(ToolkitResource):
    external_id: str = Field(description="The external ID provided by the client.")
    name: str | None = Field(default=None, description="Human-readable name for the external data source.")
    data_set_external_id: str | None = Field(
        default=None,
        description="External ID of the data set that owns this external data source.",
    )
    settings: OneLakeSettingsYAML = Field(description="OneLake connection settings.")

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)
