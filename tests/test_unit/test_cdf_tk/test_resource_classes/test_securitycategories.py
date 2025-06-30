from collections.abc import Iterable
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes import SecurityCategoriesYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.test_unit.utils import find_resources


def invalid_security_category_test_cases() -> Iterable:
    yield pytest.param(
        {"externalId": "MyCategory"},
        {"Missing required field: 'name'", "Unused field: 'externalId'"},
        id="Missing required field: name",
    )


class TestSecuritytCategoriesYAML:
    @pytest.mark.parametrize("data", list(find_resources("SecurityCategory")))
    def test_load_valid_security_categories(self, data: dict[str, object]) -> None:
        loaded = SecurityCategoriesYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data, expected_errors", list(invalid_security_category_test_cases()))
    def test_invalid_asset_error_messages(self, data: dict | list, expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, SecurityCategoriesYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert set(format_warning.errors) == expected_errors
