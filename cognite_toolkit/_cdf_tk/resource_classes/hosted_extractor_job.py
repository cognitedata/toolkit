from abc import ABC
from typing import Literal

from pydantic import Field, JsonValue

from .base import BaseModelResource, ToolkitResource


class JobFormat(BaseModelResource, ABC): ...


class MQTTConfig(BaseModelResource):
    topic_filter: str = Field(description="Topic filter")


class KafkaConfig(BaseModelResource):
    topic: str = Field(description="Kafka topic to connect to", max_length=200)
    partitions: int = Field(1, description="Number of partitions on the topic.", ge=1, le=10)


class RestConfig(BaseModelResource):
    interval: Literal["5m", "15m", "1h", "6h", "12h", "1d"]
    path: str = Field(
        description="Path of resource to access on the server, without query.", min_length=1, max_length=2048
    )
    method: Literal["get", "post"] = Field("get", description="HTTP method to use for each request.")
    body: dict[str, JsonValue] | None = Field(
        None,
        description="Initial JSON body to send with request. Only applicable if method is post. Maximum of 10000 bytes total.",
    )
    query: dict[str, str] | None = Field(
        None,
        description="Query parameters to include in request. String key -> String value. Limits: Maximum 255 characters per key, 2048 per value, and at most 32 pairs.",
        max_length=32,
    )
    headers: dict[str, str] | None = Field(
        None,
        description="HTTP headers to include in request. String key -> String value. Limits: Maximum 255 characters per key, 2048 per value, and at most 32 pairs.",
        max_length=32,
    )


class HostedExtractorJobYAML(ToolkitResource):
    external_id: str = Field(
        description="The external ID provided by the client. Must be unique for the resource type.",
        max_length=255,
    )
    destination_id: str = Field(
        description="ID of the destination this job should write to.",
        max_length=255,
    )
    source_id: str = Field(
        description="ID of the source this job should read from.",
        max_length=255,
    )
    format: JobFormat = Field(
        description="The format of the messages from the source. This is used to convert messages coming from the source system to a format that can be inserted into CDF.",
    )
    config: MQTTConfig | KafkaConfig | RestConfig | None = Field(
        None,
        description="Source specific job configuration. The type depends on the type of source, and is required for some sources.",
    )
