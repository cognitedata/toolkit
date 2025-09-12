import sys
from types import MappingProxyType
from typing import Any, ClassVar, Literal, cast

from pydantic import Field, ModelWrapValidatorHandler, model_serializer, model_validator
from pydantic_core.core_schema import SerializationInfo, SerializerFunctionWrapHandler

from cognite_toolkit._cdf_tk.constants import (
    DM_EXTERNAL_ID_PATTERN,
    SPACE_FORMAT_PATTERN,
)
from cognite_toolkit._cdf_tk.utils.collection import humanize_collection

from .base import BaseModelResource

if sys.version_info < (3, 11):
    from typing_extensions import Self
else:
    from typing import Self


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


class ConstraintDefinition(BaseModelResource):
    constraint_type: ClassVar[str]

    @model_validator(mode="wrap")
    @classmethod
    def find_constraint_definition_cls(cls, data: Any, handler: ModelWrapValidatorHandler[Self]) -> Self:
        if isinstance(data, ConstraintDefinition):
            return cast(Self, data)
        if not isinstance(data, dict):
            raise ValueError(f"Invalid constraint definition data '{type(data)}' expected dict")

        if cls is not ConstraintDefinition:
            data_copy = dict(data)
            data_copy.pop("constraintType", None)
            return handler(data_copy)

        constraint_type = data.get("constraintType")
        if constraint_type is None:
            raise ValueError("Missing 'constraintType' field in constraint data")
        if constraint_type not in _CONSTRAINT_DEFINITION_CLASS_BY_TYPE:
            raise ValueError(
                f"invalid destination type '{constraint_type}'. Expected one of {humanize_collection(_CONSTRAINT_DEFINITION_CLASS_BY_TYPE.keys(), bind_word='or')}"
            )
        cls_ = _CONSTRAINT_DEFINITION_CLASS_BY_TYPE[constraint_type]
        data_copy = dict(data)
        data_copy.pop("constraintType")

        return cast(Self, cls_.model_validate(data_copy))

    @model_serializer(mode="wrap", when_used="always", return_type=dict)
    def serialize_constrain_definition(self, handler: SerializerFunctionWrapHandler) -> dict:
        serialized_data = handler(self)
        serialized_data["constraintType"] = self.__class__.constraint_type
        return serialized_data


class UniquenessConstraintDefinition(ConstraintDefinition):
    constraint_type = "uniqueness"
    properties: list[str] = Field(description="List of properties included in the constraint.")
    by_space: bool | None = Field(default=None, description="Whether to make the constraint space-specific.")


class RequiresConstraintDefinition(ConstraintDefinition):
    constraint_type = "requires"
    require: ContainerReference = Field(description="Reference to an existing container.")


_CONSTRAINT_DEFINITION_CLASS_BY_TYPE: MappingProxyType[str, type[ConstraintDefinition]] = MappingProxyType(
    {c.constraint_type: c for c in ConstraintDefinition.__subclasses__()}
)


class IndexDefinition(BaseModelResource):
    index_type: ClassVar[str]
    properties: list[str] = Field(description="List of properties to define the index across.")

    @model_validator(mode="wrap")
    @classmethod
    def find_index_definition_cls(cls, data: Any, handler: ModelWrapValidatorHandler[Self]) -> Self:
        if isinstance(data, IndexDefinition):
            return cast(Self, data)
        if not isinstance(data, dict):
            raise ValueError(f"Invalid index definition data '{type(data)}' expected dict")

        if cls is not IndexDefinition:
            data_copy = dict(data)
            data_copy.pop("indexType", None)
            return handler(data_copy)

        index_type = data.get("indexType")
        if index_type is None:
            raise ValueError("Missing 'indexType' field in index definition data")
        if index_type not in _INDEX_DEFINITION_CLASS_BY_TYPE:
            raise ValueError(
                f"invalid index type '{index_type}'. Expected one of {humanize_collection(_INDEX_DEFINITION_CLASS_BY_TYPE.keys(), bind_word='or')}"
            )
        cls_ = _INDEX_DEFINITION_CLASS_BY_TYPE[index_type]
        data_copy = dict(data)
        data_copy.pop("indexType")
        return cast(Self, cls_.model_validate(data_copy))

    @model_serializer(mode="wrap", when_used="always", return_type=dict)
    def serialize_index(self, handler: SerializerFunctionWrapHandler) -> dict:
        serialized_data = handler(self)
        serialized_data["indexType"] = self.__class__.index_type
        return serialized_data


class BtreeIndex(IndexDefinition):
    index_type = "btree"
    by_space: bool | None = Field(default=None, description="Whether to make the index space-specific.")
    cursorable: bool | None = Field(
        default=None, description="Whether the index can be used for cursor-based pagination."
    )


class InvertedIndex(IndexDefinition):
    index_type = "inverted"


_INDEX_DEFINITION_CLASS_BY_TYPE: MappingProxyType[str, type[IndexDefinition]] = MappingProxyType(
    {c.index_type: c for c in IndexDefinition.__subclasses__()}
)


class PropertyTypeDefinition(BaseModelResource):
    type: ClassVar[str]

    @model_validator(mode="wrap")
    @classmethod
    def find_property_type_cls(cls, data: Any, handler: ModelWrapValidatorHandler[Self]) -> Self:
        if isinstance(data, PropertyTypeDefinition):
            return cast(Self, data)
        if not isinstance(data, dict):
            raise ValueError(f"Invalid property type data '{type(data)}' expected dict")

        if cls is not PropertyTypeDefinition:
            data_copy = dict(data)
            data_copy.pop("type", None)
            return handler(data_copy)

        property_type = data.get("type")
        if property_type is None:
            raise ValueError("Missing 'type' field in property type data")
        if property_type not in _PROPERTY_TYPE_CLASS_BY_TYPE:
            raise ValueError(
                f"invalid property type '{property_type}'. Expected one of {humanize_collection(_PROPERTY_TYPE_CLASS_BY_TYPE.keys(), bind_word='or')}"
            )
        cls_ = _PROPERTY_TYPE_CLASS_BY_TYPE[property_type]
        data_copy = dict(data)
        data_copy.pop("type")
        return cast(Self, cls_.model_validate(data_copy))

    @model_serializer(mode="wrap", when_used="always", return_type=dict)
    def serialize_property_type(self, handler: SerializerFunctionWrapHandler) -> dict:
        serialized_data = handler(self)
        serialized_data["type"] = self.__class__.type
        return serialized_data


class ListablePropertyTypeDefinition(PropertyTypeDefinition):
    type: ClassVar[str]
    list: bool | None = Field(
        default=None,
        description="Specifies that the data type is a list of values.",
    )
    max_list_size: int | None = Field(
        default=None,
        description="Specifies the maximum number of values in the list",
    )


class TextProperty(ListablePropertyTypeDefinition):
    type = "text"
    collation: str | None = Field(
        default=None,
        description="he set of language specific rules - used when sorting text fields.",
    )


class FloatPrimitiveProperty(ListablePropertyTypeDefinition):
    type: ClassVar[Literal["float32", "float64"]]
    unit: dict[Literal["externalId", "sourceUnit"], str] | None = Field(
        default=None,
        description="The unit of the data stored in this property.",
    )


class Float32PrimitiveProperty(FloatPrimitiveProperty):
    type = "float32"


class Float64PrimitiveProperty(FloatPrimitiveProperty):
    type = "float64"


class BooleanPrimitiveProperty(ListablePropertyTypeDefinition):
    type = "boolean"


class Int32PrimitiveProperty(ListablePropertyTypeDefinition):
    type = "int32"


class Int64PrimitiveProperty(ListablePropertyTypeDefinition):
    type = "int64"


class TimestampPrimitiveProperty(ListablePropertyTypeDefinition):
    type = "timestamp"


class DatePrimitiveProperty(ListablePropertyTypeDefinition):
    type = "date"


class JSONPrimitiveProperty(ListablePropertyTypeDefinition):
    type = "json"


class TimeseriesCDFExternalIdReference(ListablePropertyTypeDefinition):
    type = "timeseries"


class FileCDFExternalIdReference(ListablePropertyTypeDefinition):
    type = "file"


class SequenceCDFExternalIdReference(ListablePropertyTypeDefinition):
    type = "sequence"


class DirectNodeRelation(ListablePropertyTypeDefinition):
    type = "direct"
    container: ContainerReference | None = Field(
        default=None,
        description="The (optional) required type for the node the direct relation points to.",
    )


class EnumProperty(PropertyTypeDefinition):
    type = "enum"
    unknown_value: str | None = Field(
        default=None,
        description="The value to use when the enum value is unknown.",
    )
    values: dict[str, dict[Literal["name", "description"], str]] = Field(
        description="A set of all possible values for the enum property."
    )


def get_all_property_type_leaf_classes(base_class: type[PropertyTypeDefinition]) -> list:
    subclasses = base_class.__subclasses__()
    result = []

    if not subclasses:
        if base_class is not PropertyTypeDefinition:
            result.append(base_class)
    else:
        for subclass in subclasses:
            result.extend(get_all_property_type_leaf_classes(subclass))

    return result


_PROPERTY_TYPE_CLASS_BY_TYPE: MappingProxyType[str, type[PropertyTypeDefinition]] = MappingProxyType(
    {
        cls.type: cls
        for cls in get_all_property_type_leaf_classes(PropertyTypeDefinition)
        if hasattr(cls, "type") and cls.type is not None
    }
)


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
    type: PropertyTypeDefinition = Field(description="The type of data you can store in this property.")

    @model_serializer(mode="wrap")
    def serialize_type(self, handler: SerializerFunctionWrapHandler, info: SerializationInfo) -> dict:
        serialized_data = handler(self)
        if self.type:
            serialized_data["type"] = self.type.model_dump(**vars(info))
        return serialized_data
