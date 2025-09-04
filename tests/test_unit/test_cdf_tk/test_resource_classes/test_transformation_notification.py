from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes.transformation_notification import TransformationNotificationYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.test_unit.utils import find_resources


class TestTransformationNotificationYAML:
    @pytest.mark.parametrize("data", list(find_resources("Notification", "transformations")))
    def test_load_valid_transformation_notification(self, data: dict[str, object]) -> None:
        loaded = TransformationNotificationYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize(
        "data, expected_errors",
        [
            pytest.param(
                {"destination": "example@email.com"},
                {"Missing required field: 'transformationExternalId'"},
                id="missing_required_field",
            ),
            pytest.param(
                {"transformationExternalId": 123, "destination": "example@emai.com"},
                {
                    "In field transformationExternalId input should be a valid string. Got 123 of "
                    "type int. Hint: Use double quotes to force string."
                },
                id="wrong_type_field",
            ),
            pytest.param(
                {
                    "destination": "example@email.com",
                    "transformationId": 123,
                    "transformationExternalId": "ext-123",
                },
                {"Unused field: 'transformationId'"},
                id="Specifying transformationId",
            ),
        ],
    )
    def test_load_invalid_transformation_notification(self, data: dict[str, object], expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, TransformationNotificationYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert set(format_warning.errors) == expected_errors
