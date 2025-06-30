import sys
from typing import Any, Literal, cast

from cognite.client.data_classes import GroupWrite
from pydantic import ModelWrapValidatorHandler, model_serializer, model_validator
from pydantic_core.core_schema import SerializationInfo, SerializerFunctionWrapHandler

from .base import ToolkitResource
from .capabilities import Capability

if sys.version_info < (3, 11):
    from typing_extensions import Self
else:
    from typing import Self


class GroupYAML(ToolkitResource):
    _cdf_resource = GroupWrite
    name: str
    capabilities: list[Capability] | None = None
    metadata: dict[str, str] | None = None

    @model_validator(mode="wrap")
    @classmethod
    def select_group_type(cls, data: Any, handler: ModelWrapValidatorHandler[Self]) -> Self:
        if cls is not GroupYAML:
            return handler(data)
        if "sourceId" in data:
            return cast(Self, ExternalGroupYAML.model_validate(data))
        elif "members" in data:
            return cast(Self, CDFGroupYAML.model_validate(data))
        raise ValueError("Missing required field: Either 'sourceId' or 'members'")

    @model_serializer(mode="wrap")
    def serialize_group(self, handler: SerializerFunctionWrapHandler, info: SerializationInfo) -> dict:
        # Capabilities are serialized as empty dicts [{}, {}, ...]
        # This issue arises because Pydantic's serialization mechanism doesn't automatically
        # handle polymorphic serialization for subclasses of Capability.
        # To address this, we include the below to explicitly calling model dump on the capabilities
        serialized_data = handler(self)
        if self.capabilities:
            serialized_data["capabilities"] = [cap.model_dump(**vars(info)) for cap in self.capabilities]
        return serialized_data


class ExternalGroupYAML(GroupYAML):
    source_id: str


class CDFGroupYAML(GroupYAML):
    members: list[str] | Literal["allUserAccounts"]
