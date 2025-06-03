from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes import TransformationScheduleYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.test_unit.utils import find_resources


class TestTransformationScheduleYAML:
    @pytest.mark.parametrize("data", list(find_resources("schedule", "transformations")))
    def test_load_valid_transformation_schedule(self, data: dict[str, object]) -> None:
        loaded = TransformationScheduleYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize(
        "data, expected_errors",
        [
            pytest.param(
                {"externalId": "fn_first_function"},
                ["Missing required field: 'interval'"],
                id="missing_required_field",
            )
        ],
    )
    def test_load_invalid_transformation_schedule(self, data: dict[str, object], expected_errors: list[str]) -> None:
        warnings = validate_resource_yaml_pydantic(data, TransformationScheduleYAML, source_file=Path("test.yaml"))
        assert len(warnings) == 1, "Only one warning should be raised"
        warning = warnings[0]
        assert isinstance(warning, ResourceFormatWarning), (
            f"Warning should be of type ResourceFormatWarning but got {type(warning)}"
        )
        assert list(warning.errors) == expected_errors, (
            f"Warning should contain the expected error {expected_errors} but got {list(warning.errors)}"
        )
