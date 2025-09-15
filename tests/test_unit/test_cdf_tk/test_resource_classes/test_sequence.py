from collections.abc import Iterable
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes.sequence import SequenceYAML
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
