from abc import ABC
from datetime import date, datetime, timezone

import pytest
from cognite.client.data_classes import Label, LabelDefinition
from cognite.client.data_classes.data_modeling import ContainerId
from cognite.client.data_classes.data_modeling.data_types import (
    Boolean,
    DirectRelation,
    Enum,
    EnumValue,
    Float32,
    Float64,
    Int32,
    Int64,
    Json,
    PropertyType,
    Text,
    Timestamp,
)
from cognite.client.data_classes.data_modeling.instances import PropertyValueWrite

from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.dtype_conversion import (
    CONVERTER_BY_DTYPE,
    DATATYPE_CONVERTER_BY_DATA_TYPE,
    asset_centric_convert_to_primary_property,
    convert_str_to_data_type,
    convert_to_primary_property,
    infer_data_type_from_value,
)
from cognite_toolkit._cdf_tk.utils.useful_types import AVAILABLE_DATA_TYPES, AssetCentric, DataType


class TestConvertToContainerProperty:
    @pytest.mark.parametrize(
        "value, type_, nullable, expected_value",
        [
            pytest.param(
                "string_value",
                Text(),
                True,
                "string_value",
                id="String to text",
            ),
            pytest.param(
                True,
                Boolean(),
                True,
                True,
                id="Bool to boolean",
            ),
            pytest.param(
                42,
                Int32(),
                True,
                42,
                id="Int to Int32",
            ),
            pytest.param(
                1234567890123,
                Int64(),
                True,
                1234567890123,
                id="Int to Int64",
            ),
            pytest.param(
                3.14,
                Float32(),
                True,
                3.14,
                id="Float to Float32",
            ),
            pytest.param(
                2.7182818284,
                Float64(),
                True,
                2.7182818284,
                id="Float to Float64",
            ),
            pytest.param(
                {"key": "value"},
                Json(),
                True,
                {"key": "value"},
                id="Dict to Json",
            ),
            pytest.param(
                "2025-07-22T12:34:56Z",
                Timestamp(),
                True,
                datetime(2025, 7, 22, 12, 34, 56, tzinfo=timezone.utc),
                id="String to Timestamp",
            ),
            pytest.param(
                "ENUM_A",
                Enum(values={"ENUM_A": EnumValue(), "ENUM_B": EnumValue()}),
                True,
                "ENUM_A",
                id="String to Enum",
            ),
            pytest.param(
                "1",
                Boolean(),
                True,
                True,
                id="String '1' to boolean True",
            ),
            pytest.param(
                "0",
                Boolean(),
                True,
                False,
                id="String '0' to boolean False",
            ),
            pytest.param(
                "-42",
                Int32(),
                True,
                -42,
                id="String '-42' to Int32",
            ),
            pytest.param(
                "0",
                Int64(),
                True,
                0,
                id="String '0' to Int64",
            ),
            pytest.param(
                "-3.14",
                Float32(),
                True,
                -3.14,
                id="String '-3.14' to Float32",
            ),
            pytest.param(
                "0.0",
                Float64(),
                True,
                0.0,
                id="String '0.0' to Float64",
            ),
            pytest.param(
                "[1, 2, 3]",
                Json(),
                True,
                [1, 2, 3],
                id="Stringified list to Json",
            ),
            pytest.param(
                '{"a": 1, "b": 2}',
                Json(),
                True,
                {"a": 1, "b": 2},
                id="Stringified dict with ints to Json",
            ),
            pytest.param(
                "2025-01-01T00:00:00Z",
                Timestamp(),
                True,
                datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                id="String to Timestamp (start of year)",
            ),
            pytest.param(
                "ENUM_B",
                Enum(values={"ENUM_A": EnumValue(), "ENUM_B": EnumValue()}),
                True,
                "ENUM_B",
                id="String to Enum (B)",
            ),
            pytest.param(
                1753193696789,
                Timestamp(),
                True,
                datetime(2025, 7, 22, 14, 14, 56, 789000, tzinfo=timezone.utc),
                id="Epoch float with milliseconds to Timestamp",
            ),
            pytest.param(
                [1, 2, 3],
                Int32(is_list=True),
                True,
                [1, 2, 3],
                id="List of int32 to Int32 list property",
            ),
            pytest.param(
                ["2025-07-22T12:34:56Z", "2025-01-01T00:00:00Z"],
                Timestamp(is_list=True),
                True,
                [
                    datetime(2025, 7, 22, 12, 34, 56, tzinfo=timezone.utc),
                    datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                ],
                id="List of ISO timestamps to Timestamp list property",
            ),
            # List property type valid cases (additional)
            pytest.param(
                "[1, 2, 3]",
                Int32(is_list=True),
                True,
                [1, 2, 3],
                id="JSON string list to Int32 list property",
            ),
            pytest.param(
                '["2025-07-22T12:34:56Z", "2025-01-01T00:00:00Z"]',
                Timestamp(is_list=True),
                True,
                [
                    datetime(2025, 7, 22, 12, 34, 56, tzinfo=timezone.utc),
                    datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                ],
                id="JSON string list to Timestamp list property",
            ),
            pytest.param(
                42,
                Int32(is_list=True),
                True,
                [42],
                id="Single int to Int32 list property",
            ),
            pytest.param(
                "2025-07-22T12:34:56Z",
                Timestamp(is_list=True),
                True,
                [datetime(2025, 7, 22, 12, 34, 56, tzinfo=timezone.utc)],
                id="Single ISO timestamp to Timestamp list property",
            ),
        ],
    )
    def test_valid_conversion(
        self,
        value: str | int | float | bool | dict | list,
        type_: PropertyType,
        nullable: bool,
        expected_value: PropertyValueWrite,
    ):
        actual = convert_to_primary_property(value, type_, nullable)

        if isinstance(expected_value, float):
            assert actual == pytest.approx(expected_value), f"Expected {expected_value}, but got {actual}"
        else:
            assert actual == expected_value, f"Expected {expected_value}, but got {actual}"

    @pytest.mark.parametrize(
        "value, type_, nullable, error_message",
        [
            pytest.param(
                None,
                Text(),
                False,
                "Cannot convert None to a non-nullable property.",
                id="None to non-nullable Text",
            ),
            pytest.param(
                "invalid_enum_value",
                Enum(values={"ENUM_A": EnumValue(), "ENUM_B": EnumValue()}),
                True,
                "Value 'invalid_enum_value' is not a valid enum value. Available values: ENUM_A and ENUM_B",
                id="Invalid string to Enum",
            ),
            pytest.param(
                "not_a_number",
                Int32(),
                True,
                "Cannot convert not_a_number to int32.",
                id="Invalid string to Int32",
            ),
            pytest.param(
                "not_a_float",
                Float32(),
                True,
                "Cannot convert not_a_float to float32.",
                id="Invalid string to Float32",
            ),
            pytest.param(
                {"key": "value"},
                Boolean(),
                True,
                "Cannot convert {'key': 'value'} to boolean.",
                id="Dict to Boolean",
            ),
            pytest.param(
                str(2**31),
                Int32(),
                True,
                "Value 2147483648 is out of range for int32.",
                id="Int32 overflow (too large)",
            ),
            pytest.param(
                str(-(2**31) - 1),
                Int32(),
                True,
                "Value -2147483649 is out of range for int32.",
                id="Int32 underflow (too small)",
            ),
            pytest.param(
                str(3.5e38),
                Float32(),
                True,
                "Value 3.5e+38 is out of range for float32.",
                id="Float32 overflow (too large)",
            ),
            pytest.param(
                str(-3.5e38),
                Float32(),
                True,
                "Value -3.5e+38 is out of range for float32.",
                id="Float32 underflow (too small)",
            ),
            pytest.param(
                "3.14",
                Int32(),
                True,
                "Cannot convert 3.14 to int32.",
                id="Float string to Int32 (invalid)",
            ),
            pytest.param(
                "true",
                Int32(),
                True,
                "Cannot convert true to int32.",
                id="Boolean string to Int32 (invalid)",
            ),
            pytest.param(
                "ENUM_C",
                Enum(values={"ENUM_A": EnumValue(), "ENUM_B": EnumValue()}),
                True,
                "Value 'enum_c' is not a valid enum value. Available values: ENUM_A and ENUM_B",
                id="Invalid enum value (not in list)",
            ),
            pytest.param(
                '{"key": "value"}',
                Int32(),
                True,
                'Cannot convert {"key": "value"} to int32.',
                id="JSON dict string to Int32 (invalid)",
            ),
            pytest.param(
                "1e100",
                Float32(),
                True,
                "Value 1e100 is out of range for float32.",
                id="Scientific notation overflow for Float32",
            ),
            pytest.param(
                "9223372036854775808",  # 2**63
                Int64(),
                True,
                "Value 9223372036854775808 is out of range for int64.",
                id="Int64 overflow (too large)",
            ),
            pytest.param(
                "-9223372036854775809",  # -(2**63) - 1
                Int64(),
                True,
                "Value -9223372036854775809 is out of range for int64.",
                id="Int64 underflow (too small)",
            ),
            pytest.param(
                [123, 456],
                Int64(is_list=False),
                True,
                "Expected a single value for int64, but got a list.",
                id="List to Int64 (invalid, not a list type)",
            ),
        ],
    )
    def test_invalid_conversion(
        self, value: str | int | float | bool | dict | list, type_: PropertyType, nullable: bool, error_message: str
    ):
        with pytest.raises(ValueError) as exc_info:
            convert_to_primary_property(value, type_, nullable)

        assert str(exc_info.value) == error_message, (
            f"Expected error message '{error_message}', but got '{exc_info.value}'"
        )

    def test_all_converters_registered(self) -> None:
        """Checks that all property types that are in the cognite-sdk have a corresponding converter."""
        existing_types: set[str] = set()
        to_check = [PropertyType]
        while to_check:
            current_type = to_check.pop()
            for subclass in current_type.__subclasses__():
                if hasattr(subclass, "_type"):
                    existing_types.add(subclass._type)
                if ABC in subclass.__bases__:
                    to_check.append(subclass)

        missing_converters = existing_types - set(CONVERTER_BY_DTYPE.keys())

        assert not missing_converters, (
            f"Missing converters for types: {humanize_collection(missing_converters)}. "
            "Please ensure all property types have a corresponding converter."
        )

    @pytest.mark.parametrize(
        "value, type_, destination_container_property, source_property, expected",
        [
            pytest.param(
                True,
                Enum(values={"numeric": EnumValue(), "string": EnumValue()}),
                (ContainerId("cdf_cdm", "CogniteTimeSeries"), "type"),
                ("timeseries", "isString"),
                "string",
                id="TimeSeries.isString to Enum conversion",
            ),
            pytest.param(
                False,
                Enum(values={"numeric": EnumValue(), "string": EnumValue()}),
                (ContainerId("cdf_cdm", "CogniteTimeSeries"), "type"),
                ("timeseries", "isString"),
                "numeric",
                id="TimeSeries.isString to Enum conversion (False case)",
            ),
            pytest.param(
                [Label("pump"), Label("mechanical")],
                Text(is_list=True),
                (ContainerId("cdf_cdm", "CogniteDescribable"), "tags"),
                ("asset", "labels"),
                ["pump", "mechanical"],
                id="Asset labels to tags list conversion",
            ),
            pytest.param(
                [Label("pump"), {"externalId": "mechanical"}, LabelDefinition("equipment")],
                Text(is_list=True),
                (ContainerId("cdf_cdm", "CogniteDescribable"), "tags"),
                ("file", "labels"),
                ["pump", "mechanical", "equipment"],
                id="Asset label to tags list conversion",
            ),
            pytest.param(
                False,
                Boolean(),
                (ContainerId("some_other_space", "SomeOtherView"), "type"),
                ("timeseries", "isString"),
                False,
                id="Non-asset-centric boolean to primary property conversion",
            ),
            pytest.param(
                "acceleration:m-per-sec2",
                DirectRelation(),
                (ContainerId("cdf_cdm", "CogniteTimeSeries"), "unit"),
                ("timeseries", "unitExternalId"),
                {"space": "cdf_cdm_units", "externalId": "acceleration:m-per-sec2"},
                id="TimeSeries unitExternalId to DirectRelation conversion",
            ),
            pytest.param(
                None,
                DirectRelation(),
                (ContainerId("cdf_cdm", "CogniteTimeSeries"), "unit"),
                ("timeseries", "unitExternalId"),
                None,
                id="TimeSeries unitExternalId to DirectRelation conversion with None value (nullable)",
            ),
        ],
    )
    def test_asset_centric_conversion(
        self,
        value: str | int | float | bool | dict | list,
        type_: PropertyType,
        destination_container_property: tuple[ContainerId, str],
        source_property: tuple[AssetCentric, str],
        expected: PropertyValueWrite,
    ):
        actual = asset_centric_convert_to_primary_property(
            value, type_, True, destination_container_property, source_property
        )

        assert actual == expected

    @pytest.mark.parametrize(
        "value, type_, destination_container_property, source_property, error_message",
        [
            pytest.param(
                "invalid_value",
                Enum(values={"numeric": EnumValue(), "string": EnumValue()}),
                (ContainerId("cdf_cdm", "CogniteTimeSeries"), "type"),
                ("timeseries", "isString"),
                "Cannot convert invalid_value to TimeSeries type. Expected a boolean value.",
                id="Invalid TimeSeries type input value conversion error",
            ),
            pytest.param(
                "not_a_list",
                Text(is_list=True),
                (ContainerId("cdf_cdm", "CogniteDescribable"), "tags"),
                ("asset", "labels"),
                "Cannot convert not_a_list to labels. Expected a list of Labels, objects, or LabelDefinitions.",
                id="List to Text conversion error",
            ),
            pytest.param(
                True,
                DirectRelation(),
                (ContainerId("cdf_cdm", "CogniteTimeSeries"), "unit"),
                ("timeseries", "unitExternalId"),
                "Cannot convert True to TimeSeries unit. Expected a string representing the externalId.",
                id="TimeSeries unitExternalId to DirectRelation conversion error",
            ),
        ],
    )
    def test_asset_centric_failed_conversion(
        self,
        value: str | int | float | bool | dict | list,
        type_: PropertyType,
        destination_container_property: tuple[ContainerId, str],
        source_property: tuple[AssetCentric, str],
        error_message: str,
    ):
        with pytest.raises(ValueError) as exc_info:
            asset_centric_convert_to_primary_property(
                value, type_, True, destination_container_property, source_property
            )

        assert str(exc_info.value) == error_message


class TestConvertStringToDataType:
    TEST_CASES = (
        pytest.param("string_value", "string", "string_value", id="String to Text"),
        pytest.param("42", "integer", 42, id="String to integer"),
        pytest.param("3.14", "float", 3.14, id="String to float"),
        pytest.param("true", "boolean", True, id="String 'true' to Boolean"),
        pytest.param("false", "boolean", False, id="String 'false' to Boolean"),
        pytest.param('{"key": "value"}', "json", {"key": "value"}, id="Stringified dict to Json"),
        pytest.param(
            "2025-07-22T12:34:56Z",
            "timestamp",
            datetime(2025, 7, 22, 12, 34, 56, tzinfo=timezone.utc),
            id="ISO timestamp to Timestamp",
        ),
        pytest.param("2025-07-22", "date", date(2025, 7, 22), id="ISO date to Date"),
    )

    @pytest.mark.parametrize(
        "value, data_type, expected_value",
        (*TEST_CASES, pytest.param(None, "string", None, id="None to string (nullable)")),
    )
    def test_convert(
        self,
        value: str | None,
        data_type: DataType,
        expected_value: str | int | float | bool | dict | list | datetime | date | None,
    ) -> None:
        result = convert_str_to_data_type(value, data_type)
        if isinstance(expected_value, float):
            assert result == pytest.approx(expected_value), f"Expected {expected_value}, but got {result}"
        else:
            assert result == expected_value, f"Expected {expected_value}, but got {result}"

    @pytest.mark.parametrize(
        "value, data_type, expected_value",
        (*TEST_CASES, pytest.param(None, "string", [], id="None to string (nullable)")),
    )
    def test_convert_array(
        self,
        value: str | None,
        data_type: DataType,
        expected_value: str | int | float | bool | dict | list | datetime | date | None,
    ) -> None:
        result = convert_str_to_data_type(value, data_type, is_array=True)
        if isinstance(expected_value, float):
            assert result == pytest.approx([expected_value]), f"Expected [{expected_value}], but got {result}"
        elif isinstance(expected_value, list):
            assert result == expected_value, f"Expected {expected_value}, but got {result}"
        else:
            assert result == [expected_value], f"Expected [{expected_value}], but got {result}"

    def test_all_data_type_converters_registered(self) -> None:
        """Checks that all data types that are in the toolkit have a corresponding converter."""
        assert AVAILABLE_DATA_TYPES == set(DATATYPE_CONVERTER_BY_DATA_TYPE.keys())


class TestInferDataTypeFromValue:
    TEST_CASES = (
        pytest.param("string_value", "string", id="String to string"),
        pytest.param("42", "integer", id="String to integer"),
        pytest.param("3.14", "float", id="String to float"),
        pytest.param("true", "boolean", id="String 'true' to boolean"),
        pytest.param("false", "boolean", id="String 'false' to boolean"),
        pytest.param('{"key": "value"}', "json", id="Stringified dict to json"),
        pytest.param("2025-07-22T12:34:56Z", "timestamp", id="ISO timestamp to timestamp"),
        pytest.param("2025-07-22", "date", id="ISO date to date"),
        # Numeric Edge Cases
        pytest.param("42.0", "float", id="Float with zero decimal"),
        pytest.param("-10", "integer", id="Negative integer"),
        # Date/Time Ambiguity
        pytest.param("2025-07-22T00:00:00Z", "timestamp", id="Midnight UTC timestamp should be timestamp"),
        # JSON Variations
        pytest.param("[]", "json", id="JSON array"),
        # Malformed Inputs (Fallback to string)
        pytest.param("3.14.15.16.17", "string", id="Malformed float falls back to string"),
        pytest.param('{"key": "value', "string", id="Malformed JSON falls back to string"),
        pytest.param("2025-99-99", "string", id="Malformed date falls back to string"),
        pytest.param("", "string", id="Empty string is string"),
    )

    @pytest.mark.parametrize("value, expected_type", TEST_CASES)
    def test_infer_data_type_from_value(self, value: str, expected_type: str) -> None:
        result, _ = infer_data_type_from_value(value, dtype="Python")
        assert result == expected_type, f"Expected {expected_type}, but got {result}"

    @pytest.mark.parametrize("value, expected_type", TEST_CASES)
    def test_infer_data_type_from_value_json(self, value: str, expected_type: str) -> None:
        expected_type = {"timestamp": "string", "date": "string"}.get(expected_type, expected_type)
        result, _ = infer_data_type_from_value(value, dtype="Json")
        assert result == expected_type, f"Expected {expected_type}, but got {result}"
