from collections.abc import Iterable
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes import FunctionsYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.test_unit.utils import find_resources


def invalid_function_test_cases() -> Iterable:
    yield pytest.param(
        {"name": "my_function"},
        {"Missing required field: 'externalId'"},
        id="Missing externalId",
    )
    yield pytest.param(
        {
            "externalId": "my_function",
            "name": "my_function",
            "runtime": "py36",
            "secrets": {f"secret_name{i}": f"super_secret{i}" for i in range(31)},
        },
        {
            "In field runtime input should be 'py39', 'py310' or 'py311'. Got 'py36'.",
            "In field secrets dictionary should have at most 30 items after validation, not 31",
        },
        id="Invalid runtime",
    )


class TestFunctionsYAML:
    @pytest.mark.parametrize("data", list(find_resources("function")))
    def test_load_valid_function(self, data: dict[str, object]) -> None:
        loaded = FunctionsYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data, expected_errors", list(invalid_function_test_cases()))
    def test_invalid_function_error_messages(self, data: dict | list, expected_errors: set[str]) -> None:
        """Test the validate_resource_yaml function for GroupYAML."""
        warning_list = validate_resource_yaml_pydantic(data, FunctionsYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert set(format_warning.errors) == expected_errors
