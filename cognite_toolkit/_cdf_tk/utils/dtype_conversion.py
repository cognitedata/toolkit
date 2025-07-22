import ctypes
from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import ClassVar, cast

from cognite.client.data_classes.data_modeling.containers import ContainerId
from cognite.client.data_classes.data_modeling.data_types import Enum, ListablePropertyType
from cognite.client.data_classes.data_modeling.instances import PropertyValueWrite
from cognite.client.data_classes.data_modeling.views import MappedProperty
from dateutil import parser

from cognite_toolkit._cdf_tk.exceptions import ToolkitNotSupported

_SPECIAL_CASES = {
    (ContainerId("cdf_cdm", "CogniteTimeSeries"), "type"),
}


def convert_to_primary_property(
    value: str | int | float | bool | dict | list | None, prop: MappedProperty
) -> PropertyValueWrite:
    """Convert a string value to the appropriate type based on the provided property type.

    Args:
        value (str | int | float | bool): The value to convert.
        prop (MappedProperty): The property definition that specifies the type to convert to.

    Returns:
        PropertyValueWrite: The converted value as a PropertyValue, or None if is_nullable is True and the value is empty.
    """
    converter: type[_Converter]
    if (prop.container, prop.container_property_identifier) in CONVERTER_BY_CONTAINER_PROPERTY:
        converter = CONVERTER_BY_CONTAINER_PROPERTY[(prop.container, prop.container_property_identifier)]
    elif prop.type._type in CONVERTER_BY_TYPE:
        converter = CONVERTER_BY_TYPE[prop.type._type]
    else:
        raise TypeError(f"Unsupported property type {prop.type}")
    if isinstance(prop.type, ListablePropertyType) and prop.type.is_list:
        raise NotImplementedError(f"Listable property type {prop.type} is not supported")
    return converter(prop).convert(value)


class _Converter(ABC):
    def __init__(self, property: MappedProperty):
        self.property = property

    @abstractmethod
    def convert(self, value: str | int | float | bool | dict | list | None) -> PropertyValueWrite:
        """Convert a value to the appropriate type."""
        raise NotImplementedError("This method should be implemented by subclasses.")


class _SpecialCaseConverter(_Converter, ABC):
    """Abstract base class for converters handling special cases."""

    container_property: ClassVar[tuple[ContainerId, str]]


class _ValueConverter(_Converter, ABC):
    type: ClassVar[str]

    def convert(self, value: str | int | float | bool | dict | list | None) -> PropertyValueWrite:
        if value is None and self.property.nullable is False:
            raise ValueError("Cannot convert None to a non-nullable property.")
        elif value is None:
            return None
        elif isinstance(value, list):
            raise ToolkitNotSupported("List values are not supported for this property type.")
        return self._convert(value)

    @abstractmethod
    def _convert(self, value: str | int | float | bool | dict) -> PropertyValueWrite:
        """Convert the value to the appropriate type."""
        raise NotImplementedError("This method should be implemented by subclasses.")


class _TextConverter(_ValueConverter):
    type = "text"

    def _convert(self, value: str | int | float | bool | dict) -> PropertyValueWrite:
        return str(value) if value is not None else None


class _BooleanConverter(_ValueConverter):
    type = "boolean"

    def _convert(self, value: str | int | float | bool | dict) -> PropertyValueWrite:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            if value.lower() in ("true", "1"):
                return True
            elif value.lower() in ("false", "0"):
                return False
        raise ValueError(f"Cannot convert {value} to boolean.")


class _Int32Converter(_ValueConverter):
    type = "int32"

    def _convert(self, value: str | int | float | bool | dict) -> PropertyValueWrite:
        if isinstance(value, int):
            output = value
        elif isinstance(value, str):
            try:
                output = int(value)
            except ValueError:
                raise ValueError(f"Cannot convert {value} to int32.")
        else:
            raise ValueError(f"Cannot convert {value} to int32.")
        try:
            output = ctypes.c_int32(output).value
        except OverflowError:
            raise ValueError(f"Value {output} is out of range for int32.")
        return output


class _Int64Converter(_ValueConverter):
    type = "int64"

    def _convert(self, value: str | int | float | bool | dict) -> PropertyValueWrite:
        if isinstance(value, int):
            output = value
        elif isinstance(value, str):
            try:
                output = int(value)
            except ValueError:
                raise ValueError(f"Cannot convert {value} to int64.")
        else:
            raise ValueError(f"Cannot convert {value} to int64.")
        try:
            output = ctypes.c_int64(output).value
        except OverflowError:
            raise ValueError(f"Value {output} is out of range for int64.")
        return output


class _Float32Converter(_ValueConverter):
    type = "float32"

    def _convert(self, value: str | int | float | bool | dict) -> PropertyValueWrite:
        if isinstance(value, float):
            output = value
        elif isinstance(value, str):
            try:
                output = float(value)
            except ValueError:
                raise ValueError(f"Cannot convert {value} to float32.")
        else:
            raise ValueError(f"Cannot convert {value} to float32.")
        try:
            output = ctypes.c_float(output).value
        except OverflowError:
            raise ValueError(f"Value {output} is out of range for float32.")
        return output


class _Float64Converter(_ValueConverter):
    type = "float64"

    def _convert(self, value: str | int | float | bool | dict) -> PropertyValueWrite:
        if isinstance(value, float):
            output = value
        elif isinstance(value, str):
            try:
                output = float(value)
            except ValueError:
                raise ValueError(f"Cannot convert {value} to float64.")
        else:
            raise ValueError(f"Cannot convert {value} to float64.")
        try:
            output = ctypes.c_double(output).value
        except OverflowError:
            raise ValueError(f"Value {output} is out of range for float64.")
        return output


class _JsonConverter(_ValueConverter):
    type = "json"

    def _convert(self, value: str | int | float | bool | dict) -> PropertyValueWrite:
        if value is None:
            return None
        if isinstance(value, dict | list):
            return value  # type: ignore[return-value]
        if isinstance(value, str):
            try:
                import json

                return json.loads(value)
            except ValueError as e:
                raise ValueError(f"Cannot convert {value} to JSON: {e}")
        raise ValueError(f"Cannot convert {value} to JSON.")


class _TimestampConverter(_ValueConverter):
    type = "timestamp"

    def _convert(self, value: str | int | float | bool | dict) -> PropertyValueWrite:
        if isinstance(value, str):
            try:
                return parser.isoparse(value)
            except ValueError as e:
                raise ValueError(f"Cannot convert {value} to timestamp: {e}")
        raise ValueError(f"Cannot convert {value} to timestamp.")


class _DateConverter(_ValueConverter):
    type = "date"

    def _convert(self, value: str | int | float | bool | dict) -> PropertyValueWrite:
        if isinstance(value, str):
            try:
                return parser.parse(value).date()
            except ValueError as e:
                raise ValueError(f"Cannot convert {value} to date: {e}")
        raise ValueError(f"Cannot convert {value} to date.")


class _EnumConverter(_ValueConverter):
    type = "enum"

    def _convert(self, value: str | int | float | bool | dict) -> PropertyValueWrite:
        type_ = cast(Enum, self.property.type)
        available_types = {enum_value.casefold(): enum_value for enum_value in type_.values.keys()}
        value = str(value).casefold()
        if value in available_types:
            return available_types[value]
        raise ValueError(f"Value {value} is not a valid enum value. Available values: {available_types}")


class _DirectRelationshipConverter(_ValueConverter):
    type = "direct"

    def _convert(self, value: str | int | float | bool | dict) -> PropertyValueWrite:
        raise ToolkitNotSupported("Direct relationship conversion is not supported.")


class _TimeSeriesReferenceConverter(_ValueConverter):
    type = "timeseries"

    def _convert(self, value: str | int | float | bool | dict) -> PropertyValueWrite:
        raise ToolkitNotSupported("Timeseries reference conversion is not supported.")


class _FileReferenceConverter(_ValueConverter):
    type = "file"

    def _convert(self, value: str | int | float | bool | dict) -> PropertyValueWrite:
        raise ToolkitNotSupported("File reference conversion is not supported.")


class _SequenceReferenceConverter(_ValueConverter):
    type = "sequence"

    def _convert(self, value: str | int | float | bool | dict) -> PropertyValueWrite:
        raise ToolkitNotSupported("Sequence reference conversion is not supported.")


CONVERTER_BY_TYPE: Mapping[str, type[_ValueConverter]] = {cls_.type: cls_ for cls_ in _ValueConverter.__subclasses__()}  # type: ignore[type-abstract]
CONVERTER_BY_CONTAINER_PROPERTY: Mapping[tuple[ContainerId, str], type[_SpecialCaseConverter]] = {
    cls_.container_property: cls_  # type: ignore[type-abstract]
    for cls_ in _SpecialCaseConverter.__subclasses__()
}
