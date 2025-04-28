from collections.abc import Iterable
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes import TimeSeriesYAML
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
            "In item [1] missing required field: 'externalId'",
        ],
        id="Invalid second element of list of timeseries",
    )

    yield pytest.param(
        [
            {"externalId": "my_timeseries", "nam": "my_timeseries"},
            {"name": "my_timeseries_2", "type": "numeric"},
        ],
        [
            "In item [0] unused field: 'nam'",
            "In item [1] missing required field: 'externalId'",
            "In item [1] unused field: 'type'",
        ],
        id="Invalid second element of list of timeseries",
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


class TestAsJsonPath:
    @pytest.mark.parametrize(
        "loc, expected",
        [
            (("a", "b", "c"), "a.b.c"),
            (("a", 1, "c"), "a[1].c"),
            (("a", 1, 2), "a[1][2]"),
            (("a",), "a"),
            ((), ""),
            ((1,), "item [1]"),
        ],
    )
    def test_as_json_path(self, loc: tuple[str | int, ...], expected: str) -> None:
        assert as_json_path(loc) == expected
