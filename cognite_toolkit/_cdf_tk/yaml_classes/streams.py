from typing import Literal

from pydantic import Field

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId

from .base import BaseModelResource, ToolkitResource


class StreamSettings(BaseModelResource):
    """Stream settings resource class."""

    # The '4yArchive' is not official, but we know it is valid for certain customers, thus
    # we include it here to avoid validation warnings for those customers.
    template: dict[Literal["name"], Literal["ImmutableTestStream", "BasicArchive", "BasicLiveData", "4yArchive"]] = (
        Field(description="Reference to a template which should be used to define initial settings for the stream.")
    )


class StreamYAML(ToolkitResource):
    """Stream YAML resource class."""

    external_id: str = Field(
        description="The external ID of the stream.",
        min_length=1,
        max_length=100,
        pattern="^[a-z]([a-z0-9_-]{0,98}[a-z0-9])?$",
    )
    settings: StreamSettings = Field(
        description="Stream settings which should be applied to a stream.",
    )

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)
