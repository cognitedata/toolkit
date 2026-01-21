from typing import Annotated, Literal

from pydantic import Field, JsonValue

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
    UpdatableRequestResource,
)

from .identifiers import ExternalId


class JobFormatDefinition(BaseModelObject):
    type: str


class PrefixConfig(BaseModelObject):
    from_topic: bool | None = None
    prefix: str | None = None


class SpaceRef(BaseModelObject):
    space: str


class CogniteFormat(JobFormatDefinition):
    type: Literal["cognite"] = "cognite"
    encoding: str | None = None
    compression: str | None = None
    prefix: PrefixConfig | None = None
    data_models: list[SpaceRef] | None = None


class CustomFormat(JobFormatDefinition):
    type: Literal["custom"] = "custom"
    encoding: str | None = None
    compression: str | None = None
    mapping_id: str


class RockwellFormat(JobFormatDefinition):
    type: Literal["rockwell"] = "rockwell"
    encoding: str | None = None
    compression: str | None = None
    prefix: PrefixConfig | None = None
    data_models: list[SpaceRef] | None = None


class ValueFormat(JobFormatDefinition):
    type: Literal["value"] = "value"
    encoding: str | None = None
    compression: str | None = None
    prefix: PrefixConfig | None = None
    data_models: list[SpaceRef] | None = None


JobFormat = Annotated[
    CogniteFormat | CustomFormat | RockwellFormat | ValueFormat,
    Field(discriminator="type"),
]


class IncrementalLoadDefinition(BaseModelObject):
    type: str


class BodyIncrementalLoad(IncrementalLoadDefinition):
    type: Literal["body"] = "body"
    value: str


class NextUrlIncrementalLoad(IncrementalLoadDefinition):
    type: Literal["nextUrl"] = "nextUrl"
    value: str


class HeaderIncrementalLoad(IncrementalLoadDefinition):
    type: Literal["headerValue"] = "headerValue"
    key: str
    value: str


class QueryParamIncrementalLoad(IncrementalLoadDefinition):
    type: Literal["queryParam"] = "queryParam"
    key: str
    value: str


class MQTTConfig(BaseModelObject):
    topic_filter: str


class KafkaConfig(BaseModelObject):
    topic: str
    partitions: int | None = None


class RestConfig(BaseModelObject):
    interval: Literal["5m", "15m", "1h", "6h", "12h", "1d"]
    path: str
    method: Literal["get", "post"] | None = None
    body: JsonValue | None = None
    query: dict[str, str] | None = None
    headers: dict[str, str] | None = None
    incremental_load: BodyIncrementalLoad | HeaderIncrementalLoad | QueryParamIncrementalLoad | None = Field(
        None, discriminator="type"
    )
    pagination: (
        BodyIncrementalLoad | NextUrlIncrementalLoad | HeaderIncrementalLoad | QueryParamIncrementalLoad | None
    ) = Field(None, discriminator="type")


class HostedExtractorJob(BaseModelObject):
    external_id: str
    destination_id: str
    source_id: str
    format: JobFormat
    config: MQTTConfig | KafkaConfig | RestConfig

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class HostedExtractorJobRequest(HostedExtractorJob, UpdatableRequestResource): ...


class HostedExtractorJobResponse(HostedExtractorJob, ResponseResource[HostedExtractorJobRequest]):
    status: str | None = None
    created_time: int
    last_updated_time: int

    def as_request_resource(self) -> HostedExtractorJobRequest:
        return HostedExtractorJobRequest.model_validate(self.dump(), extra="ignore")
