import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast

from pydantic import ModelWrapValidatorHandler, model_serializer, model_validator
from pydantic_core.core_schema import SerializationInfo, SerializerFunctionWrapHandler

from cognite_toolkit._cdf_tk.client.identifiers import NameId
from cognite_toolkit._cdf_tk.client.resource_classes.group import GroupAttributes

from .base import ToolkitResource
from .capabilities import Capability, UnknownCapability

if TYPE_CHECKING:
    from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import ModelSyntaxWarning

if sys.version_info < (3, 11):
    from typing_extensions import Self
else:
    from typing import Self


class GroupYAML(ToolkitResource):
    name: str
    capabilities: list[Capability] | None = None
    metadata: dict[str, str] | None = None
    attributes: GroupAttributes | None = None

    def as_id(self) -> NameId:
        return NameId(name=self.name)

    def syntax_warnings(self, source_file: Path) -> "list[ModelSyntaxWarning]":
        # Lazy import to avoid circular dependency (yaml_classes → commands.build_v2 → resource_ios → yaml_classes).
        from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import ModelSyntaxWarning

        return [
            ModelSyntaxWarning(
                code="MODEL-SYNTAX-WARNING",
                message=f"Unknown capability name '{cap.original_name}'. "
                "It will be deployed as-is, but may be rejected by CDF.",
                source_files=[source_file],
                fix="Compare the YAML with reference documentation. The resource will still be deployed.",
            )
            for cap in (self.capabilities or [])
            if isinstance(cap, UnknownCapability)
        ]

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
