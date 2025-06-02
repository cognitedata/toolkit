import re
from typing import Literal

from pydantic import Field, field_validator

from cognite_toolkit._cdf_tk.constants import (
    CONTAINER_EXTERNAL_ID_PATTERN,
    CONTAINER_PROPERTIES_IDENTIFIER_PATTERN,
    FORBIDDEN_CONTAINER_EXTERNAL_IDS,
    FORBIDDEN_CONTAINER_PROPERTIES_IDENTIFIER,
    SPACE_FORMAT_PATTERN,
)
from cognite_toolkit._cdf_tk.utils.collection import humanize_collection

from .base import BaseModelResource, ToolkitResource


class ContainerReference(BaseModelResource):
    type: Literal["container"] = "container"
    space: str = Field(
        description="Id of the space hosting (containing) the container.",
        min_length=1,
        max_length=43,
        pattern=SPACE_FORMAT_PATTERN,
    )
    external_id: str = Field(
        description="External-id of the container.",
        min_length=1,
        max_length=255,
        pattern=CONTAINER_EXTERNAL_ID_PATTERN,
    )


class UniquenessConstraintDefinition(BaseModelResource):
    constraint_type: Literal["uniqueness"] = "uniqueness"
    properties: list[str] = Field(description="List of properties included in the constraint.")
    by_space: bool = Field(default=False, description="Whether to make the constraint space-specific.")


class RequiresConstraintDefinition(BaseModelResource):
    constraint_type: Literal["requires"] = "requires"
    require: ContainerReference = Field(description="Reference to an existing container.")


class BtreeIndex(BaseModelResource):
    index_type: Literal["btree"] = "btree"
    properties: list[str] = Field(description="List of properties to define the index across.")
    by_space: bool = Field(default=False, description="Whether to make the index space-specific.")
    cursorable: bool = Field(default=False, description="Whether the index can be used for cursor-based pagination.")


class InvertedIndex(BaseModelResource):
    index_type: Literal["inverted"] = "inverted"
    properties: list[str] = Field(description="List of properties to define the index across.")


class TextProperty(BaseModelResource):
    type: Literal["text"] = "text"
    list: bool = Field(
        default=False,
        description="Specifies that the data type is a list of values.",
    )
    max_list_size: int | None = Field(
        default=None,
        description="Specifies the maximum number of values in the list",
    )
    collation: str = Field(
        default="ucs_basic",
        description="he set of language specific rules - used when sorting text fields.",
    )


class PrimitiveProperty(BaseModelResource):
    type: Literal["boolean", "float32", "float64", "int32", "int64", "timestamp", "date", "json"]
    list: bool = Field(
        default=False,
        description="Specifies that the data type is a list of values.",
    )
    max_list_size: int | None = Field(
        default=None,
        description="Specifies the maximum number of values in the list",
    )
    unit: dict[Literal["externalId", "sourceUnit"], str] | None = Field(
        default=None,
        description="The unit of the data stored in this property.",
    )


class CDFExternalIdReference(BaseModelResource):
    type: Literal["timeseries", "file", "sequence"]
    list: bool = Field(
        default=False,
        description="Specifies that the data type is a list of values.",
    )
    max_list_size: int | None = Field(
        default=None,
        description="Specifies the maximum number of values in the list",
    )


class DirectNodeRelation(BaseModelResource):
    type: Literal["direct"] = "direct"
    list: bool = Field(
        default=False,
        description="Specifies that the data type is a list of values.",
    )
    max_list_size: int | None = Field(
        default=None,
        description="Specifies the maximum number of values in the list",
    )
    container: ContainerReference | None = Field(
        default=None,
        description="The (optional) required type for the node the direct relation points to.",
    )


class EnumProperty(BaseModelResource):
    type: Literal["enum"] = "enum"
    unknown_value: str | None = Field(
        default=None,
        description="The value to use when the enum value is unknown.",
    )
    values: dict[str, dict[Literal["name", "description"], str]] = Field(
        description="A set of all possible values for the enum property."
    )


class ContainerPropertyDefinition(BaseModelResource):
    immutable: bool = Field(
        default=False,
        description="Should updates to this property be rejected after the initial population?",
    )
    nullable: bool = Field(
        default=True,
        description="Does this property need to be set to a value, or not?",
    )
    auto_increment: bool = Field(
        default=False,
        description="Increment the property based on its highest current value (max value).",
    )
    default_value: str | int | bool | dict | None = Field(
        default=None,
        description="Default value to use when you do not specify a value for the property.",
    )
    description: str | None = Field(
        default=None,
        description="Description of the content and suggested use for this property.",
        max_length=1024,
    )
    name: str | None = Field(
        default=None,
        description="Readable property name.",
        max_length=255,
    )
    type: TextProperty | PrimitiveProperty | CDFExternalIdReference | DirectNodeRelation | EnumProperty = Field(
        description="The type of data you can store in this property."
    )


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
        pattern=CONTAINER_EXTERNAL_ID_PATTERN,
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
    constraints: dict[str, UniquenessConstraintDefinition | RequiresConstraintDefinition] | None = Field(
        default=None,
        description="Set of constraints to apply to the container.",
    )
    indexes: dict[str, BtreeIndex | InvertedIndex] | None = Field(
        default=None,
        description="Set of indexes to apply to the container.",
        max_length=10,
    )

    @field_validator("external_id")
    @classmethod
    def check_forbidden_external_id_value(cls, val: str) -> str:
        """Check the external_id not present in forbidden set"""
        if val in FORBIDDEN_CONTAINER_EXTERNAL_IDS:
            raise ValueError(
                f"'{val}' is a reserved container External ID. Reserved External IDs are: {humanize_collection(FORBIDDEN_CONTAINER_EXTERNAL_IDS)}"
            )
        return val

    @field_validator("properties")
    @classmethod
    def validate_properties_identifier(cls, val: dict[str, str]) -> dict[str, str]:
        """Validate properties Identifier"""
        key_pattern = re.compile(CONTAINER_PROPERTIES_IDENTIFIER_PATTERN)
        for key in val.keys():
            if not key_pattern.match(key):
                raise ValueError(f"Property '{key}' does not match the required pattern: {key_pattern.pattern}")
            if key in FORBIDDEN_CONTAINER_PROPERTIES_IDENTIFIER:
                raise ValueError(
                    f"'{key}' is a reserved property identifier. Reserved identifiers are: {humanize_collection(FORBIDDEN_CONTAINER_PROPERTIES_IDENTIFIER)}"
                )
        return val
