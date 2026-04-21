from abc import ABC
from typing import Annotated, Any, Literal

from pydantic import BeforeValidator, ConfigDict, Field, TypeAdapter, field_serializer
from pydantic_core.core_schema import FieldSerializationInfo

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.client.identifiers import ContainerId, ViewId
from cognite_toolkit._cdf_tk.utils._auxiliary import registry_from_subclasses_with_type_field


class PropertyTypeDefinition(BaseModelObject, ABC):
    type: str


class ListablePropertyTypeDefinition(PropertyTypeDefinition, ABC):
    list: bool | None = None
    max_list_size: int | None = None


class TextProperty(ListablePropertyTypeDefinition):
    type: Literal["text"] = "text"
    max_text_size: int | None = None
    collation: str | None = None


class Unit(BaseModelObject):
    external_id: str
    source_unit: str | None = None


class FloatProperty(ListablePropertyTypeDefinition, ABC):
    unit: Unit | None = None


class Float32Property(FloatProperty):
    type: Literal["float32"] = "float32"


class Float64Property(FloatProperty):
    type: Literal["float64"] = "float64"


class BooleanProperty(ListablePropertyTypeDefinition):
    type: Literal["boolean"] = "boolean"


class Int32Property(ListablePropertyTypeDefinition):
    type: Literal["int32"] = "int32"


class Int64Property(ListablePropertyTypeDefinition):
    type: Literal["int64"] = "int64"


class TimestampProperty(ListablePropertyTypeDefinition):
    type: Literal["timestamp"] = "timestamp"


class DateProperty(ListablePropertyTypeDefinition):
    type: Literal["date"] = "date"


class JSONProperty(ListablePropertyTypeDefinition):
    type: Literal["json"] = "json"


class TimeseriesCDFExternalIdReference(ListablePropertyTypeDefinition):
    type: Literal["timeseries"] = "timeseries"


class FileCDFExternalIdReference(ListablePropertyTypeDefinition):
    type: Literal["file"] = "file"


class SequenceCDFExternalIdReference(ListablePropertyTypeDefinition):
    type: Literal["sequence"] = "sequence"


class DirectNodeRelation(ListablePropertyTypeDefinition):
    type: Literal["direct"] = "direct"
    container: ContainerId | None = None
    # This property is only available in the response object. It will be ignored in the request object.
    # In the request object, use ViewCoreProperty.source instead.
    source: ViewId | None = Field(None, exclude=True)

    @field_serializer("container", mode="plain", when_used="unless-none")
    @classmethod
    def serialize_require(cls, container: ContainerId, info: FieldSerializationInfo) -> dict[str, Any]:
        return {**container.model_dump(**vars(info)), "type": "container"}


class EnumValue(BaseModelObject):
    name: str | None = None
    description: str | None = None


class EnumProperty(PropertyTypeDefinition):
    type: Literal["enum"] = "enum"
    unknown_value: str | None = None
    values: dict[str, EnumValue]


class UnknownPropertyType(PropertyTypeDefinition):
    type: str


def _handle_unknown_property_type(value: Any) -> Any:
    if isinstance(value, dict):
        prop_type = value.get("type")
        if prop_type not in _PROPERTY_TYPE_BY_TYPE:
            return UnknownPropertyType.model_validate(value)
        return _PROPERTY_TYPE_BY_TYPE[prop_type].model_validate(value)
    return value


_PROPERTY_TYPE_BY_TYPE = registry_from_subclasses_with_type_field(
    PropertyTypeDefinition,
    type_field="type",
    exclude=(UnknownPropertyType,),
)


DataType = Annotated[
    TextProperty
    | Float32Property
    | Float64Property
    | BooleanProperty
    | Int32Property
    | Int64Property
    | TimestampProperty
    | DateProperty
    | JSONProperty
    | TimeseriesCDFExternalIdReference
    | FileCDFExternalIdReference
    | SequenceCDFExternalIdReference
    | EnumProperty
    | DirectNodeRelation
    | UnknownPropertyType,
    BeforeValidator(_handle_unknown_property_type),
]

DataTypeAdapter: TypeAdapter[DataType] = TypeAdapter(DataType)
