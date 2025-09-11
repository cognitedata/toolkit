import re
from typing import Literal

from pydantic import Field, field_validator, model_serializer
from pydantic_core.core_schema import SerializationInfo, SerializerFunctionWrapHandler

from cognite_toolkit._cdf_tk.constants import (
    CONTAINER_AND_VIEW_PROPERTIES_IDENTIFIER_PATTERN,
    DM_EXTERNAL_ID_PATTERN,
    FORBIDDEN_CONTAINER_AND_VIEW_EXTERNAL_IDS,
    FORBIDDEN_CONTAINER_AND_VIEW_PROPERTIES_IDENTIFIER,
    SPACE_FORMAT_PATTERN,
)
from cognite_toolkit._cdf_tk.utils.collection import humanize_collection

from .base import ToolkitResource
from .container_field_definitions import ConstraintDefinition, ContainerPropertyDefinition, IndexDefinition

KEY_PATTERN = re.compile(CONTAINER_AND_VIEW_PROPERTIES_IDENTIFIER_PATTERN)


class ContainerYAML(ToolkitResource):
    space: str = Field(
        description="The workspace for the container, a unique identifier for the space.",
        min_length=1,
        max_length=43,
        pattern=SPACE_FORMAT_PATTERN,
    )
    external_id: str = Field(
        description="External-id of the container.",
        min_length=1,
        max_length=255,
        pattern=DM_EXTERNAL_ID_PATTERN,
    )
    name: str | None = Field(
        default=None,
        description="name for the container.",
        max_length=255,
    )
    description: str | None = Field(
        default=None,
        description="Description of the container.",
        max_length=1024,
    )
    used_for: Literal["node", "edge", "all"] | None = Field(
        default=None,
        description="Should this operation apply to nodes, edges or both.",
    )
    properties: dict[str, ContainerPropertyDefinition] = Field(
        description="Set of properties to apply to the container."
    )
    constraints: dict[str, ConstraintDefinition] | None = Field(
        default=None,
        description="Set of constraints to apply to the container.",
    )
    indexes: dict[str, IndexDefinition] | None = Field(
        default=None,
        description="Set of indexes to apply to the container.",
        max_length=10,
    )

    @field_validator("external_id")
    @classmethod
    def check_forbidden_external_id_value(cls, val: str) -> str:
        """Check the external_id not present in forbidden set"""
        if val in FORBIDDEN_CONTAINER_AND_VIEW_EXTERNAL_IDS:
            raise ValueError(
                f"'{val}' is a reserved container External ID. Reserved External IDs are: {humanize_collection(FORBIDDEN_CONTAINER_AND_VIEW_EXTERNAL_IDS)}"
            )
        return val

    @field_validator("properties")
    @classmethod
    def validate_properties_identifier(cls, val: dict[str, str]) -> dict[str, str]:
        """Validate properties Identifier"""
        for key in val.keys():
            if not KEY_PATTERN.match(key):
                raise ValueError(f"Property '{key}' does not match the required pattern: {KEY_PATTERN.pattern}")
            if key in FORBIDDEN_CONTAINER_AND_VIEW_PROPERTIES_IDENTIFIER:
                raise ValueError(
                    f"'{key}' is a reserved property identifier. Reserved identifiers are: {humanize_collection(FORBIDDEN_CONTAINER_AND_VIEW_PROPERTIES_IDENTIFIER)}"
                )
        return val

    @model_serializer(mode="wrap")
    def serialize_container(self, handler: SerializerFunctionWrapHandler, info: SerializationInfo) -> dict:
        serialized_data = handler(self)
        if self.constraints:
            serialized_data["constraints"] = {k: v.model_dump(**vars(info)) for k, v in self.constraints.items()}
        if self.indexes:
            serialized_data["indexes"] = {k: v.model_dump(**vars(info)) for k, v in self.indexes.items()}
        return serialized_data
