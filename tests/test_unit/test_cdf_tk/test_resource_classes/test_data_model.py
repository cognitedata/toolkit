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

    def test_invalid_data_model_missing_required_fields(self) -> None:
        """Test that missing required fields are properly reported."""
        invalid_data = {
            "space": "my_space",
            "externalId": "my_model",
            # Missing 'version'
        }

        warning_list = validate_resource_yaml_pydantic(invalid_data, DataModelYAML, Path("test.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)
        assert any("Missing required field: 'version'" in error for error in format_warning.errors)
