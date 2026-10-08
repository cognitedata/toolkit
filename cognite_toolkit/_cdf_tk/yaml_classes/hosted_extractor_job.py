from abc import ABC
from typing import Annotated, Literal

from pydantic import Field, JsonValue

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.constants import SPACE_FORMAT_PATTERN

from .base import BaseModelResource, ToolkitResource


class JobFormat(BaseModelResource, ABC):
    type: str
    encoding: Literal["utf8", "utf16", "utf16le", "latin1"] = Field(
        "utf8", description="The type of encoding to convert from."
    )
    compression: Literal["gzip"] = Field(
        "gzip",
        description="The compression applied to incoming messages. The messages are decompressed before being passed to transformations. This is usually not relevant for REST, where this is handled automatically, but MQTT, Kafka, and EventHub have no such mechanisms.",
    )


class CustomFormat(JobFormat):
    type: Literal["custom"] = Field("custom")
    mapping_id: str = Field(description="ID of the mapping this format should be tied to.", max_length=255)


class PrefixConfig(BaseModelResource):
    from_topic: bool | None = Field(None, description="Generate the prefix based on the topic of the received message.")
    prefix: str | None = Field(
        None,
        description="A fixed prefix to the generated IDs.",
        max_length=255,
    )


class SpaceRef(BaseModelResource):
    space: str = Field(
        description="The data models space where time series will be created.",
        min_length=1,
        max_length=43,
        pattern=SPACE_FORMAT_PATTERN,
    )


class DataModelFormat(JobFormat, ABC):
    prefix: PrefixConfig | None = Field(
        None,
        description="Generate a prefix for resources created using this format. If both prefix and fromTopic are set, the generated ID will be on the form [prefix][topic][id].",
    )
    data_models: list[SpaceRef] | None = Field(
        None,
        description="Data models configuration to specify the space for all instances.",
        max_length=10,
    )


class CogniteFormat(DataModelFormat):
    type: Literal["cognite"] = Field("cognite")


class RockwellFormat(DataModelFormat):
    type: Literal["rockwell"] = Field("rockwell")


class ValueFormat(DataModelFormat):
    type: Literal["value"] = Field("value")


JobFormatType = Annotated[
    CustomFormat | CogniteFormat | RockwellFormat | ValueFormat,
    Field(discriminator="type"),
]


class MQTTConfig(BaseModelResource):
    topic_filter: str = Field(description="Topic filter")


class KafkaConfig(BaseModelResource):
    topic: str = Field(description="Kafka topic to connect to", max_length=200)
    partitions: int = Field(1, description="Number of partitions on the topic.", ge=1, le=10)


class IncrementalLoad(BaseModelResource, ABC):
    type: str


class BodyIncrementalLoad(IncrementalLoad):
    type: Literal["body"] = Field("body")
    value: str = Field(
        "Expression yielding next message body. Note that body-based pagination is not allowed to be used if method is not set to post."
    )


class HeaderValueIncrementalLoad(IncrementalLoad):
    type: Literal["headerValue"] = Field("headerValue")
    key: str = Field("Key to insert the generated value into")
    value: str = Field("Expression that will be evaluated, and its result used as a header value.")


class NextURLIncrementalLoad(IncrementalLoad):
    type: Literal["nextUrl"] = Field("nextUrl")
    value: str = Field("Expression yielding the next URL to call.")


class QueryParameterIncrementalLoad(IncrementalLoad):
    type: Literal["queryParameter"] = Field("queryParameter")
    key: str = Field("Key to insert the generated value into")
    value: str = Field("Expression that will be evaluated, and its result used as a query parameter")


IncrementalLoadType = Annotated[
    BodyIncrementalLoad | HeaderValueIncrementalLoad | NextURLIncrementalLoad | QueryParameterIncrementalLoad,
    Field(discriminator="type"),
]

RequestIncrementalLoad = Annotated[
    BodyIncrementalLoad | HeaderValueIncrementalLoad | QueryParameterIncrementalLoad,
    Field(discriminator="type"),
]


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
    incremental_load: RequestIncrementalLoad | None = Field(
        None,
        description="The format of the messages from the source. This is used to convert messages coming from the source system to a format that can be inserted into CDF.",
    )
    pagination: IncrementalLoadType | None = Field(
        None,
        description="The format of the messages from the source. This is used to convert messages coming from the source system to a format that can be inserted into CDF.",
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
    format: JobFormatType = Field(
        description="The format of the messages from the source. This is used to convert messages coming from the source system to a format that can be inserted into CDF.",
    )
    config: MQTTConfig | KafkaConfig | RestConfig | None = Field(
        None,
        description="Source specific job configuration. The type depends on the type of source, and is required for some sources.",
    )

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)
