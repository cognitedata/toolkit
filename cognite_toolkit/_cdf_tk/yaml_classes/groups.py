from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, Literal

from pydantic import Discriminator, Field, Tag, model_serializer
from pydantic.functional_validators import BeforeValidator
from pydantic_core.core_schema import SerializationInfo, SerializerFunctionWrapHandler

from cognite_toolkit._cdf_tk.client.identifiers import NameId
from cognite_toolkit._cdf_tk.client.resource_classes.group import GroupAttributes

from .base import ToolkitResource
from .capabilities import Capability, UnknownCapability

if TYPE_CHECKING:
    from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import ModelSyntaxWarning


class BaseGroupYAML(ToolkitResource):
    group_type: str = Field(
        exclude=True
    )  # Not part of the YAML, but used to determine which subclass to use for validation
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


class ExternalGroupYAML(BaseGroupYAML):
    group_type: Literal["external"] = Field("external", exclude=True)
    source_id: str


class CDFGroupYAML(BaseGroupYAML):
    group_type: Literal["cdf"] = Field("cdf", exclude=True)
    members: list[str] | Literal["allUserAccounts"]


def _check_group_definition(data: Any) -> Any:
    if isinstance(data, dict) and "sourceId" in data and "members" in data:
        raise ValueError(
            "Invalid group definition: Cannot have both 'sourceId' and 'members'. Please specify only one."
        )
    return data


def _discriminate_group_type(data: Any) -> str | None:
    """The group type is determined by the presence of 'sourceId' (external) or 'members' (CDF)."""
    if isinstance(data, BaseGroupYAML):
        return data.group_type
    if isinstance(data, dict):
        if "sourceId" in data:
            return "external"
        elif "members" in data:
            return "cdf"
    return None


GroupYAML = Annotated[
    Annotated[CDFGroupYAML, Tag("cdf")] | Annotated[ExternalGroupYAML, Tag("external")],
    Discriminator(
        _discriminate_group_type,
        custom_error_type="missing_group_type",
        custom_error_message="Missing required field: Either 'sourceId' or 'members'",
    ),
    BeforeValidator(_check_group_definition),
]
