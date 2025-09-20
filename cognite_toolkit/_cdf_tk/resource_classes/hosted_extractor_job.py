import sys
from abc import ABC
from types import MappingProxyType
from typing import Any, ClassVar, Literal, cast

from pydantic import Field, JsonValue, ModelWrapValidatorHandler, field_validator, model_serializer, model_validator
from pydantic_core.core_schema import SerializationInfo, SerializerFunctionWrapHandler

from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses

from .base import BaseModelResource, ToolkitResource

if sys.version_info < (3, 11):
    from typing_extensions import Self
else:
    from typing import Self


class JobFormat(BaseModelResource, ABC):
    type: ClassVar[str]
    encoding: Literal["utf8", "utf16", "utf16le", "latin1"] = Field(
        "utf8", description="The type of encoding to convert from."
    )
    compression: Literal["gzip"] = Field(
        "gzip",
        description="The compression applied to incoming messages. The messages are decompressed before being passed to transformations. This is usually not relevant for REST, where this is handled automatically, but MQTT, Kafka, and EventHub have no such mechanisms.",
    )

    @model_validator(mode="wrap")
    @classmethod
    def find_format(cls, data: "dict[str, Any] | JobFormat", handler: ModelWrapValidatorHandler[Self]) -> Self:
        if isinstance(data, JobFormat):
            return cast(Self, data)
        if not isinstance(data, dict):
            raise ValueError(f"Invalid input for format '{type(data)}' expected dict")

        if cls is not JobFormat:
            # We are already in a subclass, so just validate as usual
            return handler(data)
        # If not we need to find the right subclass based on the type field.
        if "type" not in data:
            raise ValueError("Invalid input format missing 'type' key")
        type_ = data["type"]
        if type_ not in _JOB_FORMAT_CLS_BY_TYPE:
            raise ValueError(
                f"invalid type '{type_}'. Expected one of {humanize_collection(_JOB_FORMAT_CLS_BY_TYPE.keys(), bind_word='or')}"
            )
        cls_ = _JOB_FORMAT_CLS_BY_TYPE[type_]
        return cast(Self, cls_.model_validate({k: v for k, v in data.items() if k != "type"}))

    @model_serializer(mode="wrap", when_used="always", return_type=dict)
    def include_type(self, handler: SerializerFunctionWrapHandler) -> dict:
        serialized_data = handler(self)
        serialized_data["type"] = self.type
        return serialized_data


class CustomFormat(JobFormat):
    type: ClassVar[str] = "custom"
    mapping_id: str = Field(description="ID of the mapping this format should be tied to.", max_length=255)


class PrefixConfig(BaseModelResource):
    from_topic: bool | None = Field(None, description="Generate the prefix based on the topic of the received message.")
    prefix: str | None = Field(
        None,
        description="A fixed prefix to the generated IDs.",
        max_length=255,
    )


class SpaceRef(BaseModelResource):
    space: str = Field(description="The data models space where time series will be created.")


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
    type: ClassVar[str] = "cognite"


class RockwellFormat(DataModelFormat):
    type: ClassVar[str] = "rockwell"


class ValueFormat(DataModelFormat):
    type: ClassVar[str] = "value"


class MQTTConfig(BaseModelResource):
    topic_filter: str = Field(description="Topic filter")


class KafkaConfig(BaseModelResource):
    topic: str = Field(description="Kafka topic to connect to", max_length=200)
    partitions: int = Field(1, description="Number of partitions on the topic.", ge=1, le=10)


class IncrementalLoad(BaseModelResource, ABC):
    type: ClassVar[str]

    @model_validator(mode="wrap")
    @classmethod
    def find_incremental_load(
        cls, data: "dict[str, Any] | IncrementalLoad", handler: ModelWrapValidatorHandler[Self]
    ) -> Self:
        if isinstance(data, IncrementalLoad):
            return cast(Self, data)
        if not isinstance(data, dict):
            raise ValueError(f"Invalid input '{type(data)}'. Expected dict")

        if cls is not IncrementalLoad:
            # We are already in a subclass, so just validate as usual
            return handler(data)
        # If not we need to find the right subclass based on the type field.
        if "type" not in data:
            raise ValueError("Invalid input format missing 'type' key")
        type_ = data["type"]
        if type_ not in _INCREMENTAL_LOAD_CLS_BY_TYPE:
            raise ValueError(
                f"invalid type '{type_}'. Expected one of {humanize_collection(_INCREMENTAL_LOAD_CLS_BY_TYPE.keys(), bind_word='or')}"
            )
        cls_ = _INCREMENTAL_LOAD_CLS_BY_TYPE[type_]
        return cast(Self, cls_.model_validate({k: v for k, v in data.items() if k != "type"}))

    @model_serializer(mode="wrap", when_used="always", return_type=dict)
    def include_type(self, handler: SerializerFunctionWrapHandler) -> dict:
        serialized_data = handler(self)
        serialized_data["type"] = self.type
        return serialized_data


class BodyIncrementalLoad(IncrementalLoad):
    type: ClassVar[str] = "body"
    value: str = Field(
        "Expression yielding next message body. Note that body-based pagination is not allowed to be used if method is not set to post."
    )


class HeaderValueIncrementalLoad(IncrementalLoad):
    type: ClassVar[str] = "headerValue"
    key: str = Field("Key to insert the generated value into")
    value: str = Field("Expression that will be evaluated, and its result used as a header value.")


class NextURLIncrementalLoad(IncrementalLoad):
    type: ClassVar[str] = "nextUrl"
    value: str = Field("Expression yielding the next URL to call.")


class QueryParameterIncrementalLoad(IncrementalLoad):
    type: ClassVar[str] = "queryParameter"
    key: str = Field("Key to insert the generated value into")
    value: str = Field("Expression that will be evaluated, and its result used as a query parameter")


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
    incremental_load: IncrementalLoad | None = Field(
        None,
        description="The format of the messages from the source. This is used to convert messages coming from the source system to a format that can be inserted into CDF.",
    )
    pagination: IncrementalLoad | None = Field(
        None,
        description="The format of the messages from the source. This is used to convert messages coming from the source system to a format that can be inserted into CDF.",
    )

    @field_validator("incremental_load", mode="after")
    def validate_incremental_load_type(cls, v: IncrementalLoad | None) -> IncrementalLoad | None:
        if v is None:
            return v
        allowed_incremental_load_types = {
            BodyIncrementalLoad.type,
            HeaderValueIncrementalLoad.type,
            QueryParameterIncrementalLoad.type,
        }
        if v.type not in allowed_incremental_load_types:
            raise ValueError(
                f"Invalid type '{v.type}'. Expected one of {humanize_collection(allowed_incremental_load_types)}"
            )
        return v

    @model_serializer(mode="wrap")
    def serialize_incremental_load(self, handler: SerializerFunctionWrapHandler, info: SerializationInfo) -> dict:
        # IncrementalLoad and pagination are serialized as empty dict
        # This issue arises because Pydantic's serialization mechanism doesn't automatically
        # handle polymorphic serialization for subclasses of IncrementalLoad.
        # To address this, we include the below to explicitly calling model dump on the input
        serialized_data = handler(self)
        if self.incremental_load is not None:
            serialized_data["incrementalLoad" if info.by_alias else "incremental_load"] = (
                self.incremental_load.model_dump(**vars(info))
            )
        if self.pagination is not None:
            serialized_data["pagination"] = self.pagination.model_dump(**vars(info))
        return serialized_data


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


_INCREMENTAL_LOAD_CLS_BY_TYPE: MappingProxyType[str, type[IncrementalLoad]] = MappingProxyType(
    {load.type: load for load in get_concrete_subclasses(IncrementalLoad)}
)
_JOB_FORMAT_CLS_BY_TYPE: MappingProxyType[str, type[JobFormat]] = MappingProxyType(
    {fmt.type: fmt for fmt in get_concrete_subclasses(JobFormat)}
)
