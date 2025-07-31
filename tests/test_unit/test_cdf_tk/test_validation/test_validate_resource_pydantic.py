from collections.abc import Iterable
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes import GroupYAML, TimeSeriesYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import as_json_path, validate_resource_yaml_pydantic


def timeseries_yaml_test_cases() -> Iterable:
    yield pytest.param(
        {"name": "my_timeseries"},
        [
            "Missing required field: 'externalId'",
        ],
        id="Missing externalId",
    )
    yield pytest.param(
        {"externalId": "my_timeseries", "type": "numeric"},
        [
            "Unused field: 'type'",
        ],
        id="Unused field type",
    )

    yield pytest.param(
        [
            {"externalId": "my_timeseries", "name": "my_timeseries"},
            {"name": "my_timeseries_2"},
        ],
        [
            "In item [2] missing required field: 'externalId'",
        ],
        id="Invalid second element of list of timeseries",
    )

    yield pytest.param(
        [
            {"externalId": "my_timeseries", "nam": "my_timeseries"},
            {"name": "my_timeseries_2", "type": "numeric"},
        ],
        [
            "In item [1] unused field: 'nam'",
            "In item [2] missing required field: 'externalId'",
            "In item [2] unused field: 'type'",
        ],
        id="Multiple issues in a list of timeseries",
    )


def group_yaml_test_cases() -> Iterable:
    yield pytest.param(
        {"sourceId": "123-345"},
        {"Missing required field: 'name'"},
        id="Missing name",
    )
    yield pytest.param(
        {
            "name": "invalid-group",
            "capabilities": [{"dataModelInstancesAcl": {"actions": ["INVALID_ACTION"]}}],
            "members": "allUserAccounts",
        },
        {
            "In capabilities[1].actions[1] input should be 'READ', 'WRITE' or 'WRITE_PROPERTIES'. Got 'INVALID_ACTION'.",
            "In capabilities[1] missing required field: 'scope'",
        },
        id="Invalid action and missing scope",
    )

    yield pytest.param(
        [
            {
                "name": "invalid-group",
                "capabilities": [
                    {"dataModelsAcl": {"actions": ["WRITE"], "scope": {"notExisting": {"spaceIds": {"my_space"}}}}}
                ],
                "members": "allUserAccounts",
            }
        ],
        {
            "In item [1].capabilities[1].scope invalid scope name 'notExisting'. Expected all or spaceIdScope",
        },
        id="Invalid scope name",
    )


class TestValidateResourceYAML:
    @pytest.mark.parametrize("data, expected_errors", list(timeseries_yaml_test_cases()))
    def test_validate_timeseries_resource_yaml(self, data: dict | list, expected_errors: list[str]) -> None:
        """Test the validate_resource_yaml function for TimeSeriesYAML."""
        warning_list = validate_resource_yaml_pydantic(data, TimeSeriesYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert format_warning.errors == tuple(expected_errors)

    @pytest.mark.parametrize("data, expected_errors", list(group_yaml_test_cases()))
    def test_validate_group_resource_yaml(self, data: dict | list, expected_errors: set[str]) -> None:
        """Test the validate_resource_yaml function for GroupYAML."""
        warning_list = validate_resource_yaml_pydantic(data, GroupYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert set(format_warning.errors) == expected_errors


class TestAsJsonPath:
    @pytest.mark.parametrize(
        "loc, expected",
        [
            (("a", "b", "c"), "a.b.c"),
            (("a", 1, "c"), "a[2].c"),
            (("a", 1, 2), "a[2][3]"),
            (("a",), "a"),
            ((), ""),
            ((1,), "item [2]"),
        ],
    )
    def test_as_json_path(self, loc: tuple[str | int, ...], expected: str) -> None:
        assert as_json_path(loc) == expected
