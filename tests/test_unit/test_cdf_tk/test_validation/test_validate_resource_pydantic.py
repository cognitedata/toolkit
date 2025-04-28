from collections.abc import Iterable
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes import TimeSeriesYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic


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


class TestValidateResourceYAML:
    @pytest.mark.parametrize("data, expected_errors", list(timeseries_yaml_test_cases()))
    def test_validate_timeseries_resource_yaml(self, data: dict | list, expected_errors: list[str]) -> None:
        """Test the validate_resource_yaml function for TimeSeriesYAML."""
        warning_list = validate_resource_yaml_pydantic(data, TimeSeriesYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert format_warning.errors == expected_errors
