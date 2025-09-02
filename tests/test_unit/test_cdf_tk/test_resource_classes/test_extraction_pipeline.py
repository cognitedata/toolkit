from collections.abc import Iterable
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes.extraction_pipeline import ExtractionPipelineYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.test_unit.utils import find_resources


def invalid_extraction_pipeline_test_cases() -> Iterable:
    yield pytest.param(
        {"externalId": "myPipeline"},
        {"Missing required field: 'name'", "Missing required field: 'dataSetExternalId'"},
        id="Missing required fields",
    )


class TestEventYAML:
    @pytest.mark.parametrize("data", list(find_resources("ExtractionPipeline")))
    def test_load_valid_extraction_pipeline(self, data: dict[str, object]) -> None:
        loaded = ExtractionPipelineYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data, expected_errors", list(invalid_extraction_pipeline_test_cases()))
    def test_invalid_extraction_pipeline_error_messages(self, data: dict | list, expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, ExtractionPipelineYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert set(format_warning.errors) == expected_errors
