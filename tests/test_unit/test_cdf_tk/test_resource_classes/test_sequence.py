from collections.abc import Iterable
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes.sequence import SequenceRowYAML, SequenceYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.test_unit.utils import find_resources


def invalid_sequence_test_cases() -> Iterable:
    yield pytest.param(
        {"name": "Sequence 1"},
        {"Missing required field: 'externalId'", "Missing required field: 'columns'"},
        id="missing-required-fields",
    )
    yield pytest.param(
        {"name": "Sequence 1", "columns": []},
        {
            "In field columns list should have at least 1 item after validation, not 0",
            "Missing required field: 'externalId'",
        },
        id="columns-list-validation-errors",
    )
    yield pytest.param(
        {
            "externalId": "seq_1",
            "columns": [
                {"name": "Column 1"},
            ],
        },
        {"In columns[1] missing required field: 'externalId'"},
        id="missing-externalId-in-column",
    )
    yield pytest.param(
        {
            "externalId": "seq_1",
            "columns": [
                {"externalId": "col_1", "valueType": "INVALID_TYPE"},
            ],
        },
        {
            "In columns[1].valueType input should be 'STRING', 'string', 'DOUBLE', 'double', 'LONG' or 'long'. Got 'INVALID_TYPE'."
        },
        id="invalid-valueType-in-column",
    )
    yield pytest.param(
        {"externalId": "seq_1", "columns": [{"externalId": "col_1"}], "unknownField": "value"},
        {"Unused field: 'unknownField'"},
        id="unknown-field-present",
    )


def valid_sequence_test_cases() -> Iterable:
    yield pytest.param(
        {
            "externalId": "minimal_sequence",
            "columns": [{"externalId": "col_1"}],
        },
        id="minimal-valid-sequence",
    )
    yield pytest.param(
        {
            "externalId": "full_sequence",
            "name": "Full Sequence",
            "description": "A complete sequence with all fields",
            "dataSetExternalId": "my_dataset",
            "assetExternalId": "my_asset",
            "metadata": {"key1": "value1", "key2": "value2"},
            "columns": [
                {
                    "externalId": "string_col",
                    "name": "String Column",
                    "description": "A string column",
                    "valueType": "STRING",
                    "metadata": {"type": "text"},
                },
                {
                    "externalId": "double_col",
                    "name": "Double Column",
                    "description": "A double precision column",
                    "valueType": "DOUBLE",
                },
                {
                    "externalId": "long_col",
                    "name": "Long Column",
                    "description": "A long integer column",
                    "valueType": "LONG",
                },
                {
                    "externalId": "string_col_2",
                    "name": "String Column",
                    "description": "A string column",
                    "valueType": "string",
                },
                {
                    "externalId": "long_col_2",
                    "name": "Long Column",
                    "description": "A long integer column",
                    "valueType": "long",
                },
                {
                    "externalId": "double_col_2",
                    "name": "Double Column",
                    "description": "A double precision column",
                    "valueType": "double",
                },
            ],
        },
        id="full-sequence-with-all-fields",
    )


class TestSequenceYAML:
    @pytest.mark.parametrize("data", list(find_resources("Sequence")))
    def test_load_valid_sequence_from_resources(self, data: dict[str, object]) -> None:
        """Test loading valid sequences from resource files."""
        loaded = SequenceYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data", list(valid_sequence_test_cases()))
    def test_valid_sequences(self, data: dict[str, object]) -> None:
        """Test various valid sequence configurations."""
        loaded = SequenceYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data, expected_errors", list(invalid_sequence_test_cases()))
    def test_invalid_sequence_error_messages(self, data: dict | list, expected_errors: set[str]) -> None:
        """Test that invalid sequences are properly rejected with appropriate error messages."""
        warning_list = validate_resource_yaml_pydantic(data, SequenceYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)
        assert set(format_warning.errors) == expected_errors


def invalid_sequence_row_test_cases() -> Iterable:
    yield pytest.param(
        {"externalId": "seq_row_1"},
        {"Missing required field: 'columns'", "Missing required field: 'rows'"},
        id="missing-required-fields",
    )
    yield pytest.param(
        {"externalId": "seq_row_1", "columns": [], "rows": []},
        {
            "In field columns list should have at least 1 item after validation, not 0",
            "In field rows list should have at least 1 item after validation, not 0",
        },
        id="empty-lists-validation-errors",
    )
    yield pytest.param(
        {
            "externalId": "seq_row_1",
            "columns": ["col1"],
            "rows": [{"values": [1, 2, 3]}],
        },
        {"In rows[1] missing required field: 'rowNumber'"},
        id="missing-rowNumber-in-row",
    )
    yield pytest.param(
        {
            "externalId": "seq_row_1",
            "columns": ["col1"],
            "rows": [{"rowNumber": -1, "values": [1, 2, 3]}],
        },
        {"In rows[1].rowNumber input should be greater than or equal to 0"},
        id="negative-rowNumber",
    )
    yield pytest.param(
        {
            "externalId": "seq_row_1",
            "columns": ["col1"],
            "rows": [{"rowNumber": 0, "values": []}],
        },
        {"In rows[1].values list should have at least 1 item after validation, not 0"},
        id="empty-values-in-row",
    )
    yield pytest.param(
        {
            "externalId": "seq_row_1",
            "columns": ["col1", "col2"],
            "rows": [{"rowNumber": 0, "values": [1]}],
        },
        {
            "Row number 0 has 1 value(s). Each row must have exactly 2 value(s) which is the same as the number of column(s)."
        },
        id="values-columns-count-mismatch",
    )
    yield pytest.param(
        {
            "externalId": "seq_row_1",
            "columns": ["col1"],
            "rows": [{"rowNumber": 0, "values": [1, 2, 3]}],
        },
        {
            "Row number 0 has 3 value(s). Each row must have exactly 1 value(s) which is the same as the number of column(s)."
        },
        id="too-many-values-for-columns",
    )
    yield pytest.param(
        {
            "externalId": "seq_row_1",
            "columns": ["col1"],
            "rows": [{"rowNumber": 0, "values": [1]}],
            "unknownField": "value",
        },
        {"Unused field: 'unknownField'"},
        id="unknown-field-present",
    )
    yield pytest.param(
        {
            "externalId": "a" * 257,
            "columns": ["col1"],
            "rows": [{"rowNumber": 0, "values": [1]}],
        },
        {"In field externalId string should have at most 256 characters"},
        id="externalId-too-long",
    )
    yield pytest.param(
        {
            "externalId": "seq_row_1",
            "columns": ["a"] * 201,
            "rows": [{"rowNumber": 0, "values": [1]}],
        },
        {"In field columns list should have at most 200 items after validation, not 201"},
        id="too-many-columns",
    )
    yield pytest.param(
        {
            "externalId": "seq_row_1",
            "columns": ["col1"],
            "rows": [{"rowNumber": 0, "values": [1]}] * 10001,
        },
        {"In field rows list should have at most 10000 items after validation, not 10001"},
        id="too-many-rows",
    )


def valid_sequence_row_test_cases() -> Iterable:
    yield pytest.param(
        {
            "externalId": "minimal_sequence_row",
            "columns": ["col1"],
            "rows": [{"rowNumber": 0, "values": [1]}],
        },
        id="minimal-valid-sequence-row",
    )
    yield pytest.param(
        {
            "externalId": "full_sequence_row",
            "columns": ["string_col", "double_col", "long_col"],
            "rows": [
                {"rowNumber": 0, "values": ["text1", 1.5, 100]},
                {"rowNumber": 2, "values": [None, None, None]},
                {"rowNumber": 3, "values": ["text3", 3.14, 300]},
            ],
        },
        id="full-sequence-row-with-multiple-rows-and-types",
    )


class TestSequenceRowYAML:
    @pytest.mark.parametrize("data", list(find_resources("SequenceRow")))
    def test_load_valid_sequence_row_file(self, data: dict[str, object]) -> None:
        """Test loading valid sequence rows from resource files."""
        loaded = SequenceRowYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data", list(valid_sequence_row_test_cases()))
    def test_valid_sequence_rows(self, data: dict[str, object]) -> None:
        """Test various valid sequence row configurations."""
        loaded = SequenceRowYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data, expected_errors", list(invalid_sequence_row_test_cases()))
    def test_invalid_sequence_row(self, data: dict | list, expected_errors: set[str]) -> None:
        """Test that invalid sequence rows are properly rejected with appropriate error messages."""
        warning_list = validate_resource_yaml_pydantic(data, SequenceRowYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)
        assert set(format_warning.errors) == expected_errors
