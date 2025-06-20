from collections.abc import Iterable
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes import DatabaseYAML, TableYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.test_unit.utils import find_resources


def invalid_database_test_cases() -> Iterable:
    yield pytest.param(
        {"name": "Asset 1"},
        {"Unused field: 'name'", "Missing required field: 'dbName'"},
        id="Missing required field: dbName",
    )
    yield pytest.param(
        {"dbName": "myDB" * 32},
        {"In field dbName string should have at most 32 characters"},
        id="Above maximum character length for dbName",
    )


def invalid_table_test_cases() -> Iterable:
    yield pytest.param(
        {"tableName": "MyTable"}, {"Missing required field: 'dbName'"}, id="Missing required field: dbName"
    )
    yield pytest.param(
        {"dbName": "myDB" * 32, "tableName": "MyTable"},
        {"In field dbName string should have at most 32 characters"},
        id="Above maximum character length for dbName",
    )


class TestRawDatabaseTableYAML:
    @pytest.mark.parametrize("data", list(find_resources("Database")))
    def test_load_valid_database(self, data: dict[str, object]) -> None:
        loaded = DatabaseYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data", list(find_resources("Table")))
    def test_load_valid_table(self, data: dict[str, object]) -> None:
        loaded = TableYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data, expected_errors", list(invalid_database_test_cases()))
    def test_invalid_database_error_messages(self, data: dict | list, expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, DatabaseYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert set(format_warning.errors) == expected_errors

    @pytest.mark.parametrize("data, expected_errors", list(invalid_table_test_cases()))
    def test_invalid_table_error_messages(self, data: dict | list, expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, TableYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert set(format_warning.errors) == expected_errors
