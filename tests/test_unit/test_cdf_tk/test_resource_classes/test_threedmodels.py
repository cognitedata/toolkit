from collections.abc import Iterable
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes import ThreeDModelYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.test_unit.utils import find_resources


def invalid_3D_test_cases() -> Iterable:
    yield pytest.param(
        {"externalId": "MyModel"},
        {"Missing required field: 'name'", "Unused field: 'externalId'"},
        id="Missing required field: externalId",
    )
    yield pytest.param(
        {"name": "MyModel", "dataSetId": 123},
        {"Unused field: 'dataSetId'"},
        id="Unused field: dataSetId",
    )
    yield pytest.param(
        {"name": "MyModel", "metadata": {f"key{i}": f"value{i}" for i in range(17)}},
        {"In field metadata dictionary should have at most 16 items after validation, not 17"},
        id="Invalid metadata types",
    )


class TestDataSetYAML:
    @pytest.mark.parametrize("data", list(find_resources("3DModel")))
    def test_load_valid_dataset(self, data: dict[str, object]) -> None:
        loaded = ThreeDModelYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data, expected_errors", list(invalid_3D_test_cases()))
    def test_invalid_asset_error_messages(self, data: dict | list, expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, ThreeDModelYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert set(format_warning.errors) == expected_errors
