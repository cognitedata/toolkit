from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes import DataModelYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.test_unit.utils import find_resources


class TestDataModelYAML:
    """Test suite for DataModelYAML class."""

    @pytest.mark.parametrize("data", list(find_resources("DataModel")))
    def test_load_valid_data_model_from_resources(self, data: dict[str, object]) -> None:
        """Test loading valid data models from resource files."""
        loaded = DataModelYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize(
        "data",
        [
            pytest.param(
                {
                    "space": "my_space",
                    "externalId": "my_model",
                    "version": "v1",
                },
                id="minimal-no-views",
            ),
            pytest.param(
                {
                    "space": "my_space",
                    "externalId": "my_model",
                    "version": "v1",
                    "views": [
                        {
                            "space": "view_space1",
                            "externalId": "view1",
                            "version": "v1",
                            "type": "view",
                        },
                        {
                            "space": "view_space2",
                            "externalId": "view2",
                            "version": "v2",
                            "type": "view",
                        },
                    ],
                },
                id="with-multiple-views",
            ),
        ],
    )
    def test_valid_data_models(self, data: dict) -> None:
        """Test various valid data model configurations."""
        loaded = DataModelYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize(
        "data,expected_errors",
        [
            pytest.param(
                {
                    "space": "my_space",
                    "externalId": "my_model",
                    # Missing 'version'
                },
                ["Missing required field: 'version'"],
                id="missing-required-field",
            ),
            pytest.param(
                {
                    "space": "",  # Empty space
                    "externalId": "my_model",
                    "version": "v1",
                },
                ["In field space string should have at least 1 character"],
                id="invalid-space",
            ),
            pytest.param(
                {
                    "space": "my_space",
                    "externalId": "a" * 256,  # External ID too long (max 255)
                    "version": "v1",
                },
                ["In field externalId string should have at most 255 characters"],
                id="invalid-external-id",
            ),
            pytest.param(
                {
                    "space": "my_space",
                    "externalId": "my_model",
                    "version": "a" * 44,  # Version too long (max 43)
                },
                ["In field version string should have at most 43 characters"],
                id="invalid-version",
            ),
            pytest.param(
                {
                    "space": "my_space",
                    "externalId": "my_model",
                    "version": "v1",
                    "name": "a" * 256,  # Name too long (max 255)
                },
                ["In field name string should have at most 255 characters"],
                id="invalid-name",
            ),
        ],
    )
    def test_invalid_data_models(self, data: dict, expected_errors: list[str]) -> None:
        """Test that invalid data models are properly rejected with appropriate error messages."""
        warning_list = validate_resource_yaml_pydantic(data, DataModelYAML, Path("test.yaml"))
        assert len(warning_list) >= 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)
        assert list(format_warning.errors) == expected_errors

    @pytest.mark.parametrize(
        "data,expected_errors",
        [
            pytest.param(
                {
                    "space": "my_space",
                    "externalId": "my_model",
                    "version": "v1",
                    "views": [
                        {
                            "space": "view_space",
                            "externalId": "my_view",
                        }
                    ],
                },
                ["In views[1] missing required field: 'version'"],
                id="view-missing-version",
            ),
            pytest.param(
                {
                    "space": "my_space",
                    "externalId": "my_model",
                    "version": "v1",
                    "views": [
                        {
                            "space": "view_space",
                            "externalId": "my_view",
                            "version": "v1",
                            "type": "invalid_type",
                        }
                    ],
                },
                ["In views[1].type input should be 'view'. Got 'invalid_type'."],
                id="view-invalid-type",
            ),
        ],
    )
    def test_invalid_views_in_data_model(self, data: dict, expected_errors: list[str]) -> None:
        """Test that data models with invalid views are properly rejected."""
        warning_list = validate_resource_yaml_pydantic(data, DataModelYAML, Path("test.yaml"))
        assert len(warning_list) >= 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)
        assert list(format_warning.errors) == expected_errors

    @pytest.mark.parametrize(
        "data,expected_errors",
        [
            pytest.param(
                {
                    "space": "my_space",
                    "externalId": "my_model",
                    "version": "v1",
                    "unknown_field": "some_value",
                },
                ["Unused field: 'unknown_field'"],
                id="single-unknown-field",
            ),
        ],
    )
    def test_data_model_with_extra_fields(self, data: dict, expected_errors: str) -> None:
        """Test that data models with extra/unused properties are properly rejected."""
        warning_list = validate_resource_yaml_pydantic(data, DataModelYAML, Path("test.yaml"))
        assert len(warning_list) >= 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)
        assert list(format_warning.errors) == expected_errors
