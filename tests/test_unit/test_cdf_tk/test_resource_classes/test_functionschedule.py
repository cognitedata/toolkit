from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes import FunctionScheduleYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.test_unit.utils import find_resources


class TestFunctionScheduleYAML:
    @pytest.mark.parametrize("data", list(find_resources("schedule", "functions")))
    def test_load_valid_function_schedule(self, data: dict[str, object]) -> None:
        loaded = FunctionScheduleYAML.model_validate(data)

        dumped = loaded.model_dump(exclude_unset=True, by_alias=True)
        assert "authentication" in dumped
        # Secret is not dumped as per design, so we add it back for comparison
        dumped["authentication"]["clientSecret"] = data["authentication"]["clientSecret"]
        assert dumped == data

    @pytest.mark.parametrize(
        "data, expected_errors",
        [
            pytest.param(
                {"functionExternalId": "fn_first_function", "name": "daily-8am-utc"},
                ["Missing required field: 'cronExpression'"],
                id="missing_required_field",
            )
        ],
    )
    def test_load_invalid_function_schedule(self, data: dict[str, object], expected_errors: list[str]) -> None:
        warnings = validate_resource_yaml_pydantic(data, FunctionScheduleYAML, source_file=Path("test.yaml"))
        assert len(warnings) == 1, "Only one warning should be raised"
        warning = warnings[0]
        assert isinstance(warning, ResourceFormatWarning), (
            f"Warning should be of type ResourceFormatWarning but got {type(warning)}"
        )
        assert list(warning.errors) == expected_errors, (
            f"Warning should contain the expected error {expected_errors} but got {list(warning.errors)}"
        )

    @pytest.mark.parametrize(
        "data",
        [
            pytest.param(
                {
                    "functionExternalId": "my_function",
                    "name": "my_schedule",
                    "cronExpression": "0 8 * * *",
                    "data": {"some": {"nested": "data"}},
                },
                id="Schedule with nested data",
            )
        ],
    )
    def test_load_valid_function(self, data: dict[str, object]) -> None:
        warnings = validate_resource_yaml_pydantic(data, FunctionScheduleYAML, source_file=Path("test.yaml"))

        assert len(warnings) == 0, "No warnings should be raised for valid function schedule"
