from pydantic import Field

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId

from .authentication import AuthenticationClientIdSecret
from .base import ToolkitResource


class HostedExtractorDestinationYAML(ToolkitResource):
    external_id: str = Field(
        description="The external ID provided by the client.",
        max_length=255,
    )
    name: str | None = Field(
        None,
        description="The name of the destination.",
        max_length=50,
    )
    description: str | None = Field(
        None,
        description="The description of the destination.",
        max_length=500,
    )

    metadata: dict[str, str] | None = Field(
        None,
        description="Custom, application specific metadata. String key -> String value. Limits are 16 key-value pairs, 512 bytes per value, and 4096 bytes total.",
        max_length=16,
    )
    credentials: AuthenticationClientIdSecret | None = Field(
        None, description="Credentials for authenticating towards CDF using a CDF session."
    )
    target_data_set_external_id: str | None = Field(
        None,
        description="The external ID of the target data set where the extractor will write data.",
        max_length=255,
    )

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)
