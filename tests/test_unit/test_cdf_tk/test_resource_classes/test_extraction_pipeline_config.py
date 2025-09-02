from collections.abc import Iterable
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes.extraction_pipeline_config import ExtractionPipelineConfigYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.test_unit.utils import find_resources


def invalid_extraction_pipeline_config_test_cases() -> Iterable:
    # Missing required field: config
    yield pytest.param(
        {"externalId": "myConfig"}, {"Missing required field: 'config'"}, id="Missing required field: config"
    )
    # Missing required field: external_id
    yield pytest.param(
        {"config": {"key": "value"}}, {"Missing required field: 'externalId'"}, id="Missing required field: externalId"
    )
    # Empty external_id
    yield pytest.param(
        {"externalId": "", "config": {"key": "value"}},
        {"In field externalId string should have at least 1 character"},
        id="Empty externalId",
    )
    # config is not a string or dict
    yield pytest.param(
        {"externalId": "myConfig", "config": 123},
        {
            "In config.dict input should be a valid dictionary. Got 123 of type int.",
            "In config.str input should be a valid string. Got 123 of type int. Hint: "
            "Use double quotes to force string.",
        },
        id="Config not string or dict",
    )
    # Unknown field present
    yield pytest.param(
        {"externalId": "myConfig", "config": {"key": "value"}, "unknownField": 1},
        {"Unused field: 'unknownField'"},
        id="Unknown field present",
    )


class TestExtractionPipelineConfigYAML:
    @pytest.mark.parametrize("data", list(find_resources("Config", resource_dir="extraction_pipelines")))
    def test_load_valid_asset(self, data: dict[str, object]) -> None:
        loaded = ExtractionPipelineConfigYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data, expected_errors", list(invalid_extraction_pipeline_config_test_cases()))
    def test_invalid_asset_error_messages(self, data: dict | list, expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, ExtractionPipelineConfigYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert set(format_warning.errors) == expected_errors
