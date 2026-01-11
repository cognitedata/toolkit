from typing import Literal

from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client.data_classes.base import (
    BaseModelObject,
    RequestResource,
    ResponseResource,
)

from .identifiers import ExternalId


class PrefixConfig(BaseModelObject):
    from_topic: bool | None = None
    prefix: str | None = None


class SpaceRef(BaseModelObject):
    space: str


class JobFormat(BaseModelObject):
    type: str | None = None
    encoding: str | None = None
    compression: str | None = None
    mapping_id: str | None = None
    prefix: PrefixConfig | None = None
    data_models: list[SpaceRef] | None = None


class IncrementalLoad(BaseModelObject):
    type: str | None = None
    key: str | None = None
    value: str | None = None


class MQTTConfig(BaseModelObject):
    topic_filter: str


class KafkaConfig(BaseModelObject):
    topic: str
    partitions: int | None = None


class RestConfig(BaseModelObject):
    interval: Literal["5m", "15m", "1h", "6h", "12h", "1d"] | None = None
    path: str | None = None
    method: Literal["get", "post"] | None = None
    body: JsonValue | None = None
    query: dict[str, str] | None = None
    headers: dict[str, str] | None = None
    incremental_load: IncrementalLoad | None = None
    pagination: IncrementalLoad | None = None


class HostedExtractorJob(BaseModelObject):
    external_id: str
    destination_id: str
    source_id: str
    format: JobFormat
    config: MQTTConfig | KafkaConfig | RestConfig

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class HostedExtractorJobRequest(HostedExtractorJob, RequestResource): ...


class HostedExtractorJobResponse(HostedExtractorJob, ResponseResource[HostedExtractorJobRequest]):
    status: str | None = None
    created_time: int
    last_updated_time: int

    def as_request_resource(self) -> HostedExtractorJobRequest:
        return HostedExtractorJobRequest.model_validate(self.dump(), extra="ignore")
