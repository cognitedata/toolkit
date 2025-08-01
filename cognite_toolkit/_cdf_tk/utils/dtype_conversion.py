import ctypes
import json
from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import ClassVar, cast

from cognite.client.data_classes.data_modeling.data_types import Enum, ListablePropertyType, PropertyType
from cognite.client.data_classes.data_modeling.instances import PropertyValueWrite
from cognite.client.utils import ms_to_datetime
from dateutil import parser

from cognite_toolkit._cdf_tk.exceptions import ToolkitNotSupported

from .collection import humanize_collection

INT32_MIN = -2_147_483_648
INT32_MAX = 2_147_483_647
INT64_MIN = -9_223_372_036_854_775_808
INT64_MAX = 9_223_372_036_854_775_807


def convert_to_primary_property(
    value: str | int | float | bool | dict | list | None, type_: PropertyType, nullable: bool
) -> PropertyValueWrite:
    """Convert a string value to the appropriate type based on the provided property type.

    Args:
        value (str | int | float | bool): The value to convert.
        type_ (PropertyType): The type of the property to convert to.
        nullable (bool): Whether the property can be null.

    Returns:
        PropertyValueWrite: The converted value as a PropertyValue, or None if is_nullable is True and the value is empty.
    """
    dtype = type_._type
    if dtype in CONVERTER_BY_DTYPE:
        converter = CONVERTER_BY_DTYPE[dtype]
    else:
        raise TypeError(f"Unsupported property type {dtype}")
    if isinstance(type_, ListablePropertyType) and type_.is_list:
        raise NotImplementedError(f"Listable property type {dtype} is not supported")
    return converter(type_, nullable).convert(value)


class _Converter(ABC):
    @abstractmethod
    def convert(self, value: str | int | float | bool | dict | list | None) -> PropertyValueWrite:
        """Convert a value to the appropriate type."""
        raise NotImplementedError("This method should be implemented by subclasses.")


class _ValueConverter(_Converter, ABC):
    type_str: ClassVar[str]

    def __init__(self, type_: PropertyType, nullable: bool):
        self.type = type_
        self.nullable = nullable

    def convert(self, value: str | int | float | bool | dict | list | None) -> PropertyValueWrite:
        if value is None and self.nullable is False:
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
    type_str = "text"

    def _convert(self, value: str | int | float | bool | dict) -> PropertyValueWrite:
        return str(value)


class _BooleanConverter(_ValueConverter):
    type_str = "boolean"

    def _convert(self, value: str | int | float | bool | dict) -> PropertyValueWrite:
        if isinstance(value, bool | int | float):
            return bool(value)
        elif isinstance(value, str):
            if value.lower() in ("true", "1"):
                return True
            elif value.lower() in ("false", "0"):
                return False
        raise ValueError(f"Cannot convert {value} to boolean.")


class _Int32Converter(_ValueConverter):
    type_str = "int32"

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
        if output < INT32_MIN or output > INT32_MAX:
            raise ValueError(f"Value {value} is out of range for int32.")
        return output


class _Int64Converter(_ValueConverter):
    type_str = "int64"

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
        if not (INT64_MIN <= output <= INT64_MAX):
            raise ValueError(f"Value {value} is out of range for int64.")
        return output


class _Float32Converter(_ValueConverter):
    type_str = "float32"

    def _convert(self, value: str | int | float | bool | dict) -> PropertyValueWrite:
        if isinstance(value, float | int):
            output = float(value)
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
            raise ValueError(f"Value {value} is out of range for float32.")
        if output == float("inf") or output == float("-inf"):
            raise ValueError(f"Value {value} is out of range for float32.")
        return output


class _Float64Converter(_ValueConverter):
    type_str = "float64"

    def _convert(self, value: str | int | float | bool | dict) -> PropertyValueWrite:
        if isinstance(value, float | int):
            output = float(value)
        elif isinstance(value, str):
            try:
                output = float(value)
            except ValueError:
                raise ValueError(f"Cannot convert {value} to float64.")
        else:
            raise ValueError(f"Cannot convert {value} to float64.")
        if output == float("inf") or output == float("-inf"):
            raise ValueError(f"Value {value} is out of range for float64.")
        return output


class _JsonConverter(_ValueConverter):
    type_str = "json"

    def _convert(self, value: str | int | float | bool | dict) -> PropertyValueWrite:
        if isinstance(value, bool | int | float):
            return value
        elif isinstance(value, dict):
            if non_string_keys := [k for k in value if not isinstance(k, str)]:
                raise ValueError(
                    f"JSON keys must be strings. Found non-string keys: {humanize_collection(non_string_keys)}"
                )
            return value  # type: ignore[return-value]
        elif isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError as e:
                raise ValueError(f"Cannot convert {value} to JSON: {e}") from e
        raise ValueError(f"Cannot convert {value} to JSON.")


class _TimestampConverter(_ValueConverter):
    type_str = "timestamp"

    def _convert(self, value: str | int | float | bool | dict) -> PropertyValueWrite:
        if isinstance(value, int | float):
            try:
                return ms_to_datetime(value)
            except (ValueError, OSError) as e:
                raise ValueError(f"Cannot convert numeric value {value} to timestamp: {e}") from e
        elif isinstance(value, str):
            try:
                return parser.isoparse(value)
            except ValueError as e:
                raise ValueError(f"Cannot convert {value} to timestamp: {e}") from e
        raise ValueError(f"Cannot convert {value} to timestamp.")


class _DateConverter(_ValueConverter):
    type_str = "date"

    def _convert(self, value: str | int | float | bool | dict) -> PropertyValueWrite:
        if isinstance(value, str):
            try:
                return parser.parse(value).date()
            except ValueError as e:
                raise ValueError(f"Cannot convert {value} to date: {e}") from e
        raise ValueError(f"Cannot convert {value} to date.")


class _EnumConverter(_ValueConverter):
    type_str = "enum"

    def _convert(self, value: str | int | float | bool | dict) -> PropertyValueWrite:
        type_ = cast(Enum, self.type)
        available_types = {enum_value.casefold(): enum_value for enum_value in type_.values.keys()}
        value = str(value).casefold()
        if value in available_types:
            return available_types[value]
        raise ValueError(
            f"Value {value!r} is not a valid enum value. Available values: {humanize_collection(available_types.values())}"
        )


class _DirectRelationshipConverter(_ValueConverter):
    type_str = "direct"

    def _convert(self, value: str | int | float | bool | dict) -> PropertyValueWrite:
        raise ToolkitNotSupported("Direct relationship conversion is not supported.")


class _TimeSeriesReferenceConverter(_ValueConverter):
    type_str = "timeseries"

    def _convert(self, value: str | int | float | bool | dict) -> PropertyValueWrite:
        raise ToolkitNotSupported("Timeseries reference conversion is not supported.")


class _FileReferenceConverter(_ValueConverter):
    type_str = "file"

    def _convert(self, value: str | int | float | bool | dict) -> PropertyValueWrite:
        raise ToolkitNotSupported("File reference conversion is not supported.")


class _SequenceReferenceConverter(_ValueConverter):
    type_str = "sequence"

    def _convert(self, value: str | int | float | bool | dict) -> PropertyValueWrite:
        raise ToolkitNotSupported("Sequence reference conversion is not supported.")


CONVERTER_BY_DTYPE: Mapping[str, type[_ValueConverter]] = {
    cls_.type_str: cls_  # type: ignore[type-abstract]
    for cls_ in _ValueConverter.__subclasses__()
}
