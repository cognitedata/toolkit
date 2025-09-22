from collections.abc import Iterable
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes.robotics.data_postprocessing import RobotDataPostProcessingYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.test_unit.utils import find_resources


def valid_robot_data_postprocessing_cases() -> Iterable:
    yield pytest.param(
        {
            "name": "Read dial gauge",
            "externalId": "read_dial_gauge",
            "method": "read_dial_gauge",
            "description": "Original Description",
            "inputSchema": {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "id": "robotics/schemas/0.1.0/capabilities/ptz",
                "title": "PTZ camera capability input",
                "type": "object",
                "properties": {
                    "method": {"type": "string"},
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "tilt": {"type": "number", "minimum": -90, "maximum": 90},
                            "pan": {"type": "number", "minimum": -180, "maximum": 180},
                            "zoom": {"type": "number", "minimum": 0, "maximum": 100},
                        },
                        "required": ["tilt", "pan", "zoom"],
                    },
                },
                "required": ["method", "parameters"],
                "additionalProperties": False,
            },
        },
        id="full-data-postprocessing",
    )
    yield pytest.param(
        {"name": "Defect Detection", "externalId": "defect_detection_001", "method": "detect_defects"},
        id="minimal-data-postprocessing",
    )


def invalid_robot_data_postprocessing_cases() -> Iterable:
    yield pytest.param(
        {"name": "Missing External ID", "method": "some_method"},
        {"Missing required field: 'externalId'"},
        id="missing-required-field",
    )
    yield pytest.param(
        {"name": "Test PostProcessing", "externalId": "postproc_001", "method": "test_method", "unknownField": "value"},
        {"Unused field: 'unknownField'"},
        id="unknown-field-present",
    )


class TestRobotDataPostProcessingYAML:
    @pytest.mark.parametrize("data", list(find_resources("DataPostProcessing", "robotics")))
    def test_load_valid_robot_data_postprocessing_file(self, data: dict[str, object]) -> None:
        loaded = RobotDataPostProcessingYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data", valid_robot_data_postprocessing_cases())
    def test_load_valid_robot_data_postprocessing(self, data: dict[str, object]) -> None:
        loaded = RobotDataPostProcessingYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data, expected_errors", invalid_robot_data_postprocessing_cases())
    def test_invalid_robot_data_postprocessing(self, data: dict | list, expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, RobotDataPostProcessingYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)
        assert set(format_warning.errors) == expected_errors
