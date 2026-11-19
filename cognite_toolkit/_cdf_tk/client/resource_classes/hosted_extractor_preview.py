from typing import Annotated, Literal

from pydantic import Field, JsonValue

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_job import KafkaConfig, MQTTConfig, RestConfig
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_mapping import MappingInput


class PreviewFormat(BaseModelObject):
    encoding: Literal["utf8", "utf16", "utf16le", "latin1"] | None = None
    compression: Literal["gzip"] | None = None


class HostedExtractorPreview(BaseModelObject):
    external_id: str
    source_id: str
    # The API expects config, but this is likely a bug in the docs.
    config: MQTTConfig | KafkaConfig | RestConfig | None = None
    format: PreviewFormat | None = None


class HostedExtractorPreviewRequest(HostedExtractorPreview):
    """Read a message from a source without creating a job, for example to test a source and a mapping input."""

    input: MappingInput | None = None


class HostedExtractorPreviewResponse(HostedExtractorPreview):
    # Protobuf files are returned without their content.
    input: dict[str, JsonValue]
    created_time: int


class HostedExtractorPreviewPending(BaseModelObject):
    type: Literal["pending"] = "pending"
    external_id: str


class HostedExtractorPreviewError(BaseModelObject):
    type: Literal["error"] = "error"
    external_id: str
    kind: Literal["startup_error", "connection_error", "transform_error"]
    message: str | None = None
    created_time: int


class HostedExtractorPreviewPayload(BaseModelObject):
    """The message read from the source, parsed with the preview input. Download the raw message separately."""

    type: Literal["preview"] = "preview"
    external_id: str
    payload: JsonValue = Field(default=None, alias="json")
    context: dict[str, JsonValue] | None = None
    created_time: int


HostedExtractorPreviewResult = Annotated[
    HostedExtractorPreviewPending | HostedExtractorPreviewError | HostedExtractorPreviewPayload,
    Field(discriminator="type"),
]
