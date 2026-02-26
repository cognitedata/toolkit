from typing import Annotated, Any, Literal

from pydantic import Field, JsonValue, field_validator

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
    UpdatableRequestResource,
)
from cognite_toolkit._cdf_tk.client.identifiers.identifiers import ExternalId


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
    config: MQTTConfig | KafkaConfig | RestConfig | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)

    @field_validator("config", mode="before")
    @classmethod
    def empty_dict_as_none(cls, value: Any) -> Any:
        if value == {}:
            return None
        return value


class HostedExtractorJobRequest(HostedExtractorJob, UpdatableRequestResource):
    def as_update(self, mode: Literal["patch", "replace"]) -> dict[str, Any]:
        update_item = super().as_update(mode=mode)
        exclude_unset = mode == "patch"
        update_item["update"]["format"] = {
            "set": self.format.model_dump(mode="json", exclude_none=True, by_alias=True, exclude_unset=exclude_unset)
        }
        return update_item


class HostedExtractorJobResponse(HostedExtractorJob, ResponseResource[HostedExtractorJobRequest]):
    status: str | None = None
    created_time: int
    last_updated_time: int

    @classmethod
    def request_cls(cls) -> type[HostedExtractorJobRequest]:
        return HostedExtractorJobRequest
