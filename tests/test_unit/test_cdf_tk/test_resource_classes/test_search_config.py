from collections.abc import Iterable
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes.search_config import SearchConfigYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.test_unit.utils import find_resources


def invalid_search_config_test_cases() -> Iterable:
    yield pytest.param(
        {"useAsName": "name_prop"},
        {"Missing required field: 'view'"},
        id="Missing required field: view",
    )
    yield pytest.param(
        {
            "view": {"space": "my_space", "externalId": "my_view"},
            "columnsLayout": [{"property": 123}],  # property should be str
        },
        {
            "In columnsLayout[1].property input should be a valid string. Got 123 of type int. "
            "Hint: Use double quotes to force string.",
        },
        id="Invalid type in columns_layout property",
    )
    yield pytest.param(
        {
            "view": {"space": "my_space", "externalId": "my_view"},
            "unknown_field": "value",
        },
        {"Unused field: 'unknown_field'"},
        id="Unused field",
    )


class TestSearchConfigYAML:
    @pytest.mark.parametrize("data", list(find_resources("SearchConfig")))
    def test_load_valid_search_config(self, data: dict[str, object]) -> None:
        loaded = SearchConfigYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data, expected_errors", list(invalid_search_config_test_cases()))
    def test_invalid_search_config_error_messages(self, data: dict | list, expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, SearchConfigYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert set(format_warning.errors) == expected_errors
