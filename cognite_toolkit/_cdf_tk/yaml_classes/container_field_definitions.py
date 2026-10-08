from abc import ABC
from typing import Annotated, Literal

from pydantic import Field

from cognite_toolkit._cdf_tk.client import identifiers
from cognite_toolkit._cdf_tk.constants import (
    DM_EXTERNAL_ID_PATTERN,
    SPACE_FORMAT_PATTERN,
)

from .base import BaseModelResource


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
        pattern=DM_EXTERNAL_ID_PATTERN,
    )

    def as_id(self) -> identifiers.ContainerId:
        """Converts the reference to a ContainerId identifier."""
        return identifiers.ContainerId(space=self.space, external_id=self.external_id)


class ConstraintDefinition(BaseModelResource):
    constraint_type: str


class UniquenessConstraintDefinition(ConstraintDefinition):
    constraint_type: Literal["uniqueness"] = "uniqueness"
    properties: list[str] = Field(description="List of properties included in the constraint.")
    by_space: bool | None = Field(default=None, description="Whether to make the constraint space-specific.")


class RequiresConstraintDefinition(ConstraintDefinition):
    constraint_type: Literal["requires"] = "requires"
    require: ContainerReference = Field(description="Reference to an existing container.")


ConstraintType = Annotated[
    UniquenessConstraintDefinition | RequiresConstraintDefinition,
    Field(discriminator="constraint_type"),
]


class IndexDefinition(BaseModelResource):
    index_type: str
    properties: list[str] = Field(description="List of properties to define the index across.")


class BtreeIndex(IndexDefinition):
    index_type: Literal["btree"] = "btree"
    by_space: bool | None = Field(default=None, description="Whether to make the index space-specific.")
    cursorable: bool | None = Field(
        default=None, description="Whether the index can be used for cursor-based pagination."
    )


class InvertedIndex(IndexDefinition):
    index_type: Literal["inverted"] = "inverted"


IndexType = Annotated[
    BtreeIndex | InvertedIndex,
    Field(discriminator="index_type"),
]


class PropertyTypeDefinition(BaseModelResource):
    type: str


class ListablePropertyTypeDefinition(PropertyTypeDefinition, ABC):
    list: bool | None = Field(
        default=None,
        description="Specifies that the data type is a list of values.",
    )
    max_list_size: int | None = Field(
        default=None,
        description="Specifies the maximum number of values in the list",
    )


class TextProperty(ListablePropertyTypeDefinition):
    type: Literal["text"] = "text"
    collation: str | None = Field(
        default=None,
        description="he set of language specific rules - used when sorting text fields.",
    )
    max_text_size: int | None = Field(
        default=None,
        description="Specifies the maximum size in bytes of the text property, when encoded with utf-8",
    )


class FloatPrimitiveProperty(ListablePropertyTypeDefinition, ABC):
    unit: dict[Literal["externalId", "sourceUnit"], str] | None = Field(
        default=None,
        description="The unit of the data stored in this property.",
    )


class Float32PrimitiveProperty(FloatPrimitiveProperty):
    type: Literal["float32"] = "float32"


class Float64PrimitiveProperty(FloatPrimitiveProperty):
    type: Literal["float64"] = "float64"


class BooleanPrimitiveProperty(ListablePropertyTypeDefinition):
    type: Literal["boolean"] = "boolean"


class Int32PrimitiveProperty(ListablePropertyTypeDefinition):
    type: Literal["int32"] = "int32"


class Int64PrimitiveProperty(ListablePropertyTypeDefinition):
    type: Literal["int64"] = "int64"


class TimestampPrimitiveProperty(ListablePropertyTypeDefinition):
    type: Literal["timestamp"] = "timestamp"


class DatePrimitiveProperty(ListablePropertyTypeDefinition):
    type: Literal["date"] = "date"


class JSONPrimitiveProperty(ListablePropertyTypeDefinition):
    type: Literal["json"] = "json"


class TimeseriesCDFExternalIdReference(ListablePropertyTypeDefinition):
    type: Literal["timeseries"] = "timeseries"


class FileCDFExternalIdReference(ListablePropertyTypeDefinition):
    type: Literal["file"] = "file"


class SequenceCDFExternalIdReference(ListablePropertyTypeDefinition):
    type: Literal["sequence"] = "sequence"


class DirectNodeRelation(ListablePropertyTypeDefinition):
    type: Literal["direct"] = "direct"
    container: ContainerReference | None = Field(
        default=None,
        description="The (optional) required type for the node the direct relation points to.",
    )


class EnumProperty(PropertyTypeDefinition):
    type: Literal["enum"] = "enum"
    unknown_value: str | None = Field(
        default=None,
        description="The value to use when the enum value is unknown.",
    )
    values: dict[str, dict[Literal["name", "description"], str]] = Field(
        description="A set of all possible values for the enum property."
    )


PropertyType = Annotated[
    TextProperty
    | Float32PrimitiveProperty
    | Float64PrimitiveProperty
    | BooleanPrimitiveProperty
    | Int32PrimitiveProperty
    | Int64PrimitiveProperty
    | TimestampPrimitiveProperty
    | DatePrimitiveProperty
    | JSONPrimitiveProperty
    | TimeseriesCDFExternalIdReference
    | FileCDFExternalIdReference
    | SequenceCDFExternalIdReference
    | DirectNodeRelation
    | EnumProperty,
    Field(discriminator="type"),
]


class ContainerPropertyDefinition(BaseModelResource):
    immutable: bool | None = Field(
        default=None,
        description="Should updates to this property be rejected after the initial population?",
    )
    nullable: bool | None = Field(
        default=None,
        description="Does this property need to be set to a value, or not?",
    )
    auto_increment: bool | None = Field(
        default=None,
        description="Increment the property based on its highest current value (max value).",
    )
    default_value: str | int | bool | dict | float | None = Field(
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
    type: PropertyType = Field(description="The type of data you can store in this property.")
