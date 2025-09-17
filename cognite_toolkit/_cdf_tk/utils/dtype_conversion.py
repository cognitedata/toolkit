import ctypes
import json
from abc import ABC, abstractmethod
from collections.abc import Mapping
from datetime import datetime
from typing import ClassVar, Literal, overload

from cognite.client.data_classes import Label, LabelDefinition
from cognite.client.data_classes.data_modeling import ContainerId
from cognite.client.data_classes.data_modeling.data_types import Enum, ListablePropertyType, PropertyType
from cognite.client.data_classes.data_modeling.instances import PropertyValueWrite
from cognite.client.utils import ms_to_datetime
from dateutil import parser

from cognite_toolkit._cdf_tk.constants import CDF_UNIT_SPACE
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotSupported
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses
from cognite_toolkit._cdf_tk.utils.useful_types import AssetCentric, DataType, JsonVal, PythonTypes

from .collection import humanize_collection

INT32_MIN = -2_147_483_648
INT32_MAX = 2_147_483_647
INT64_MIN = -9_223_372_036_854_775_808
INT64_MAX = 9_223_372_036_854_775_807


def asset_centric_convert_to_primary_property(
    value: str | int | float | bool | dict | list | None,
    type_: PropertyType,
    nullable: bool,
    destination_container_property: tuple[ContainerId, str],
    source_property: tuple[AssetCentric, str],
) -> PropertyValueWrite:
    if (source_property, destination_container_property) in SPECIAL_CONVERTER_BY_SOURCE_DESTINATION:
        converter_cls = SPECIAL_CONVERTER_BY_SOURCE_DESTINATION[(source_property, destination_container_property)]
        return converter_cls().convert(value)
    else:
        # Fallback to the standard conversion
        return convert_to_primary_property(value, type_, nullable)


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
        converter_cls = CONVERTER_BY_DTYPE[dtype]
    else:
        raise TypeError(f"Unsupported property type {dtype}")
    if issubclass(converter_cls, _EnumConverter) and isinstance(type_, Enum):
        converter: _ValueConverter = _EnumConverter(type_=type_, nullable=nullable)
    elif issubclass(converter_cls, _EnumConverter):
        raise TypeError(f"Unsupported property type {type_} for enum conversion")
    else:
        converter = converter_cls(nullable)

    if isinstance(type_, ListablePropertyType) and type_.is_list:
        values = _as_list(value)
        output: list[PropertyValueWrite] = []
        for item in values:
            converted = converter.convert(item)  # type: ignore[arg-type]
            if converted is not None:
                output.append(converted)
        # MyPy gets confused by the SequenceNotStr used in the PropertyValueWrite
        return output  # type: ignore[return-value]
    else:
        return converter.convert(value)


def convert_str_to_data_type(
    value: str | None,
    type_: DataType,
    nullable: bool = True,
    is_array: bool = False,
) -> PythonTypes | None:
    """Convert a string value to the appropriate data type based on the provided type.

    Args:
        value: The value to convert, which can be a string or None.
        type_: The target data type.
        nullable: Whether data type can be null.
        is_array: Whether the data type is an array.

    Returns:
        The converted value as a string, int, float, bool, datetime, date, dict, list, or None.

    """
    if type_ not in DATATYPE_CONVERTER_BY_DATA_TYPE:
        raise ValueError(
            f"Unsupported data type {type_}. Available types: {humanize_collection(DATATYPE_CONVERTER_BY_DATA_TYPE.keys())}."
        )
    converter_cls = DATATYPE_CONVERTER_BY_DATA_TYPE[type_]
    converter = converter_cls(nullable)
    if is_array:
        values = _as_list(value)
        output: list[PythonTypes] = []
        for item in values:
            converted = converter.convert(item)  # type: ignore[arg-type]
            if converted is not None:
                output.append(converted)  # type: ignore[arg-type]
        # MyPy gets confused by the SequenceNotStr used in the PropertyValueWrite
        return output
    else:
        return converter.convert(value)  # type: ignore[return-value]


@overload
def infer_data_type_from_value(value: str, dtype: Literal["Json"]) -> tuple[DataType, JsonVal]: ...


@overload
def infer_data_type_from_value(value: str, dtype: Literal["Python"]) -> tuple[DataType, PythonTypes]: ...


def infer_data_type_from_value(value: str, dtype: Literal["Json", "Python"]) -> tuple[DataType, JsonVal | PythonTypes]:
    """Infer the data type from a given value.

    Args:
        value: The value to infer the data type from, which can be a string.
        dtype: The data type to infer. Can be "JsonVal" or "Python". Python type will infer datetime and date types
            as well.

    Returns:
        A tuple containing the inferred data type and the converted value.

    """
    converter_classes = (
        _Int64Converter,
        _Float64Converter,
        _TimestampConverter,
        _BooleanConverter,
        _JsonConverter,
        _TextConverter,
    )
    converters_to_use = (
        tuple(converter_cls for converter_cls in converter_classes if converter_cls is not _TimestampConverter)
        if dtype == "Json"
        else converter_classes
    )
    for converter_cls in converters_to_use:
        # MyPy thinks that converter_cls can be abstract, but it is not
        converter = converter_cls(nullable=False)
        try:
            converted_value = converter.convert(value)
        except ValueError:
            continue

        if (
            converter_cls is _TimestampConverter
            and isinstance(converted_value, datetime)
            and _is_midnight_and_naive(converted_value)
        ):
            # If the converted value is a datetime with no time component, return it as a date
            return _DateConverter.schema_type, converted_value.date()
        else:
            return converter_cls.schema_type, converted_value  # type: ignore[return-value]

    raise ValueError(
        f"Failed to infer data type from value: {value!r}. Supported types are: "
        f"{humanize_collection(DATATYPE_CONVERTER_BY_DATA_TYPE.keys())}."
    )


def _is_midnight_and_naive(dt: datetime) -> bool:
    """Checks if a datetime object is at midnight and is naive (has no timezone)."""
    return not (dt.hour or dt.minute or dt.second or dt.microsecond or dt.tzinfo)


def _as_list(value: str | int | float | bool | dict[str, object] | list[object] | None) -> list[object]:
    """Convert a value to a list, ensuring that it is iterable."""
    if value is None:
        return []
    elif isinstance(value, list):
        return value
    elif isinstance(value, str) and value.strip() == "":
        return []
    elif isinstance(value, str):
        try:
            data = json.loads(value)
            return data if isinstance(data, list) else [data]
        except json.JSONDecodeError:
            return [value]
    elif isinstance(value, int | float | bool | dict):
        return [value]
    else:
        raise TypeError(f"Cannot convert {value} of type {type(value)} to a list.")


class _Converter(ABC):
    @abstractmethod
    def convert(self, value: str | int | float | bool | dict | list | None) -> PropertyValueWrite:
        """Convert a value to the appropriate type."""
        raise NotImplementedError("This method should be implemented by subclasses.")


class _ValueConverter(_Converter, ABC):
    type_str: ClassVar[str]
    schema_type: ClassVar[DataType | None] = None
    _handles_list: ClassVar[bool] = False

    def __init__(self, nullable: bool):
        self.nullable = nullable

    def convert(self, value: str | int | float | bool | dict | list | None) -> PropertyValueWrite:
        if value is None and self.nullable is False:
            raise ValueError("Cannot convert None to a non-nullable property.")
        elif value is None:
            return None
        elif isinstance(value, list) and not self._handles_list:
            raise ValueError(f"Expected a single value for {self.type_str}, but got a list.")
        # If the value is a list, we handle it in the subclass if it supports lists.
        return self._convert(value)  # type: ignore[arg-type]

    @abstractmethod
    def _convert(self, value: str | int | float | bool | dict) -> PropertyValueWrite:
        """Convert the value to the appropriate type."""
        raise NotImplementedError("This method should be implemented by subclasses.")


class _SpecialCaseConverter(_Converter, ABC):
    """Abstract base class for converters handling special cases."""

    source_property: ClassVar[tuple[AssetCentric, str]]
    destination_container_property: ClassVar[tuple[ContainerId, str]]


class _TimeSeriesTypeConverter(_SpecialCaseConverter):
    source_property = ("timeseries", "isString")
    destination_container_property = (ContainerId("cdf_cdm", "CogniteTimeSeries"), "type")

    def convert(self, value: str | int | float | bool | dict | list | None) -> str:
        if isinstance(value, bool):
            return "string" if value else "numeric"
        raise ValueError(f"Cannot convert {value} to TimeSeries type. Expected a boolean value.")


class _TimeSeriesUnitConverter(_SpecialCaseConverter):
    source_property = ("timeseries", "unitExternalId")
    destination_container_property = (ContainerId("cdf_cdm", "CogniteTimeSeries"), "unit")

    def convert(self, value: str | int | float | bool | dict | list | None) -> dict[str, str] | None:
        if value is None:
            return None
        if isinstance(value, str):
            return {"space": CDF_UNIT_SPACE, "externalId": value}
        raise ValueError(f"Cannot convert {value!r} to TimeSeries unit. Expected a string representing the externalId.")


class _LabelConverter(_SpecialCaseConverter, ABC):
    destination_container_property = (ContainerId("cdf_cdm", "CogniteDescribable"), "tags")

    def convert(self, value: str | int | float | bool | dict | list | None) -> list[str]:
        if not isinstance(value, list):
            raise ValueError(
                f"Cannot convert {value} to labels. Expected a list of Labels, objects, or LabelDefinitions."
            )
        tags: list[str] = []
        for item in value:
            match item:
                case str() as tag:
                    tags.append(tag)
                # Matches a dict with the key "externalId" whose value is a string
                case {"externalId": str() as tag}:
                    tags.append(tag)
                # Matches Label or LabelDefinition objects with a truthy external_id
                case Label(external_id=tag) | LabelDefinition(external_id=tag) if tag:
                    tags.append(tag)
                case _:
                    raise ValueError(
                        f"Invalid label item: {item}. Expected a string, dict with 'externalId', or Label/LabelDefinition."
                    )
        return tags


class _AssetLabelConverter(_LabelConverter):
    source_property = ("asset", "labels")


class _FileLabelConverter(_LabelConverter):
    source_property = ("file", "labels")


class _TextConverter(_ValueConverter):
    type_str = "text"
    schema_type = "string"

    def _convert(self, value: str | int | float | bool | dict) -> PropertyValueWrite:
        return str(value)


class _BooleanConverter(_ValueConverter):
    type_str = "boolean"
    schema_type = "boolean"

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
    schema_type = "integer"

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
    schema_type = "float"

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
    schema_type = "json"
    _handles_list = True

    def _convert(self, value: str | int | float | bool | dict[str, object] | list) -> PropertyValueWrite:
        if isinstance(value, bool | int | float):
            return value
        elif isinstance(value, dict):
            if non_string_keys := [k for k in value if not isinstance(k, str)]:
                raise ValueError(
                    f"JSON keys must be strings. Found non-string keys: {humanize_collection(non_string_keys)}"
                )
            return value
        elif isinstance(value, list):
            if not all(isinstance(item, str | int | float | bool | dict | list) for item in value):
                raise ValueError("All items in the list must be of type str, int, float, bool, dict, or list.")
            return value
        elif isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError as e:
                raise ValueError(f"Cannot convert {value} to JSON: {e}") from e
        raise ValueError(f"Cannot convert {value} to JSON.")


class _TimestampConverter(_ValueConverter):
    type_str = "timestamp"
    schema_type = "timestamp"

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
    schema_type = "date"

    def _convert(self, value: str | int | float | bool | dict) -> PropertyValueWrite:
        if isinstance(value, str):
            try:
                return parser.parse(value).date()
            except ValueError as e:
                raise ValueError(f"Cannot convert {value} to date: {e}") from e
        raise ValueError(f"Cannot convert {value} to date.")


class _EnumConverter(_ValueConverter):
    type_str = "enum"

    def __init__(self, type_: Enum, nullable: bool) -> None:
        super().__init__(nullable)
        self.available_types = {enum_value.casefold(): enum_value for enum_value in type_.values.keys()}

    def _convert(self, value: str | int | float | bool | dict) -> PropertyValueWrite:
        value = str(value).casefold()
        if value in self.available_types:
            return self.available_types[value]
        raise ValueError(
            f"Value {value!r} is not a valid enum value. Available values: {humanize_collection(self.available_types.values())}"
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
SPECIAL_CONVERTER_BY_SOURCE_DESTINATION: Mapping[
    tuple[tuple[AssetCentric, str], tuple[ContainerId, str]],
    type[_SpecialCaseConverter],
] = {
    (subclass.source_property, subclass.destination_container_property): subclass
    for subclass in get_concrete_subclasses(_SpecialCaseConverter)  # type: ignore[type-abstract]
}
DATATYPE_CONVERTER_BY_DATA_TYPE: Mapping[DataType, type[_ValueConverter]] = {
    cls_.schema_type: cls_
    for cls_ in get_concrete_subclasses(_ValueConverter)  # type: ignore[type-abstract]
    if cls_.schema_type is not None
}
