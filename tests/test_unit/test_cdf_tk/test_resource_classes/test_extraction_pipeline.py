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
    # Missing externalId
    yield pytest.param(
        {"name": "Pipeline 2", "dataSetExternalId": "ds1"},
        {"Missing required field: 'externalId'"},
        id="Missing externalId",
    )
    # Empty name
    yield pytest.param(
        {"externalId": "pipeline3", "name": "", "dataSetExternalId": "ds2"},
        {"In field name string should have at least 1 character"},
        id="Empty name",
    )
    # Invalid dataSetExternalId type
    yield pytest.param(
        {"externalId": "pipeline4", "name": "Pipeline 4", "dataSetExternalId": 123},
        {
            "In field dataSetExternalId input should be a valid string. Got 123 of type "
            "int. Hint: Use double quotes to force string."
        },
        id="Invalid dataSetExternalId type",
    )
    # All required fields present but with extra unknown field
    yield pytest.param(
        {"externalId": "pipeline5", "name": "Pipeline 5", "dataSetExternalId": "ds5", "unknownField": "value"},
        {"Unused field: 'unknownField'"},
        id="Unknown field present",
    )


class TestExtractionPipelineYAML:
    @pytest.mark.parametrize(
        "data",
        [
            *find_resources("ExtractionPipeline"),
            {
                "externalId": "pipeline6",
                "name": "Pipeline 6",
                "dataSetExternalId": "ds6",
                "contacts": [{"name": "John Doe", "role": "Owner", "sendNotification": True}],
                "notificationConfig": {"allowedNotSeenRangeInMinutes": 10},
            },
        ],
    )
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
