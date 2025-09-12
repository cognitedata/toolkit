from pydantic import Field

from .authentication import AuthenticationClientIdSecret
from .base import ToolkitResource


class HostedExtractorDestinationYAML(ToolkitResource):
    external_id: str = Field(
        description="The external ID provided by the client.",
        max_length=255,
    )
    credentials: AuthenticationClientIdSecret | None = Field(
        None, description="Credentials for authenticating towards CDF using a CDF session."
    )
    target_data_set_external_id: str | None = Field(
        None,
        description="The external ID of the target data set where the extractor will write data.",
        max_length=255,
    )
