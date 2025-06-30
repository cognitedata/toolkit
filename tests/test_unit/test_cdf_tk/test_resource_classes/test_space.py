from collections.abc import Iterable
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes import SpaceYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.test_unit.utils import find_resources


def invalid_space_test_cases() -> Iterable:
    yield pytest.param(
        {"externalId": "mySpace"},
        {"Unused field: 'externalId'", "Missing required field: 'space'"},
        id="Missing required field: space",
    )
    yield pytest.param(
        {"space": "space"},
        {
            "In field space 'space' is a reserved space. Reserved Spaces: cdf, dms, edge, "
            "node, pg3, shared, space and system"
        },
        id="Reserved name",
    )
    yield pytest.param(
        {
            "space": "invalid<characters>",
        },
        {"In field space string should match pattern '^[a-zA-Z][a-zA-Z0-9_-]{0,41}[a-zA-Z0-9]?$'"},
        id="Invalid characters in space name",
    )


class TestSpaceYAML:
    @pytest.mark.parametrize("data", list(find_resources("Space")))
    def test_load_valid_space(self, data: dict[str, object]) -> None:
        loaded = SpaceYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data, expected_errors", list(invalid_space_test_cases()))
    def test_invalid_asset_error_messages(self, data: dict | list, expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, SpaceYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert set(format_warning.errors) == expected_errors
