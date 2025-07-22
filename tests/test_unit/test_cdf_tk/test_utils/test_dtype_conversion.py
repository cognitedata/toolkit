from collections.abc import Mapping

import pytest
from cognite.client.data_classes.data_modeling import ContainerId, MappedProperty
from cognite.client.data_classes.data_modeling.data_types import (
    Boolean,
    Enum,
    EnumValue,
    Float32,
    Float64,
    Int32,
    Int64,
    Json,
    Text,
    Timestamp,
)
from cognite.client.data_classes.data_modeling.instances import PropertyValueWrite

from cognite_toolkit._cdf_tk.utils.dtype_conversion import convert_to_primary_property


class TestConvertToContainerProperty:
    default_properties: Mapping = dict(
        container=ContainerId("some_space", "some_container"),
        container_property_identifier="does_not_matter",
        immutable=False,
        auto_increment=True,
    )

    @pytest.mark.parametrize(
        "value, prop, expected_value",
        [
            pytest.param(
                "string_value",
                MappedProperty(type=Text(), nullable=True, **default_properties),
                "string_value",
                id="String to text",
            ),
            pytest.param(
                True, MappedProperty(type=Boolean(), nullable=True, **default_properties), True, id="Bool to boolean"
            ),
            pytest.param(42, MappedProperty(type=Int32(), nullable=True, **default_properties), 42, id="Int to Int32"),
            pytest.param(
                1234567890123,
                MappedProperty(type=Int64(), nullable=True, **default_properties),
                1234567890123,
                id="Int to Int64",
            ),
            pytest.param(
                3.14, MappedProperty(type=Float32(), nullable=True, **default_properties), 3.14, id="Float to Float32"
            ),
            pytest.param(
                2.7182818284,
                MappedProperty(type=Float64(), nullable=True, **default_properties),
                2.7182818284,
                id="Float to Float64",
            ),
            pytest.param(
                {"key": "value"},
                MappedProperty(type=Json(), nullable=True, **default_properties),
                {"key": "value"},
                id="Dict to Json",
            ),
            pytest.param(
                "2025-07-22T12:34:56Z",
                MappedProperty(type=Timestamp(), nullable=True, **default_properties),
                "2025-07-22T12:34:56Z",
                id="String to Timestamp",
            ),
            pytest.param(
                "ENUM_A",
                MappedProperty(
                    type=Enum(values={"ENUM_A": EnumValue(), "ENUM_B": EnumValue()}),
                    nullable=True,
                    **default_properties,
                ),
                "ENUM_A",
                id="String to Enum",
            ),
            pytest.param(
                "1",
                MappedProperty(type=Boolean(), nullable=True, **default_properties),
                True,
                id="String '1' to boolean True",
            ),
            pytest.param(
                "0",
                MappedProperty(type=Boolean(), nullable=True, **default_properties),
                False,
                id="String '0' to boolean False",
            ),
            pytest.param(
                "-42",
                MappedProperty(type=Int32(), nullable=True, **default_properties),
                -42,
                id="String '-42' to Int32",
            ),
            pytest.param(
                "0", MappedProperty(type=Int64(), nullable=True, **default_properties), 0, id="String '0' to Int64"
            ),
            pytest.param(
                "-3.14",
                MappedProperty(type=Float32(), nullable=True, **default_properties),
                -3.14,
                id="String '-3.14' to Float32",
            ),
            pytest.param(
                "0.0",
                MappedProperty(type=Float64(), nullable=True, **default_properties),
                0.0,
                id="String '0.0' to Float64",
            ),
            pytest.param(
                "[1, 2, 3]",
                MappedProperty(type=Json(), nullable=True, **default_properties),
                [1, 2, 3],
                id="Stringified list to Json",
            ),
            pytest.param(
                '{"a": 1, "b": 2}',
                MappedProperty(type=Json(), nullable=True, **default_properties),
                {"a": 1, "b": 2},
                id="Stringified dict with ints to Json",
            ),
            pytest.param(
                "2025-01-01T00:00:00Z",
                MappedProperty(type=Timestamp(), nullable=True, **default_properties),
                "2025-01-01T00:00:00Z",
                id="String to Timestamp (start of year)",
            ),
            pytest.param(
                "ENUM_B",
                MappedProperty(
                    type=Enum(values={"ENUM_A": EnumValue(), "ENUM_B": EnumValue()}),
                    nullable=True,
                    **default_properties,
                ),
                "ENUM_B",
                id="String to Enum (B)",
            ),
        ],
    )
    def test_valid_conversion(
        self, value: str | int | float | bool | dict | list, prop: MappedProperty, expected_value: PropertyValueWrite
    ):
        actual = convert_to_primary_property(value, prop)

        assert actual == expected_value, f"Expected {expected_value}, but got {actual}"

    def test_invalid_conversion(
        self, value: str | int | float | bool | dict | list, prop: MappedProperty, error_message: str
    ):
        with pytest.raises(ValueError) as exc_info:
            convert_to_primary_property(value, prop)

        assert str(exc_info.value) == error_message, (
            f"Expected error message '{error_message}', but got '{exc_info.value}'"
        )
