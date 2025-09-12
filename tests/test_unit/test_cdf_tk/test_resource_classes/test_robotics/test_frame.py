from collections.abc import Iterable
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes.robotics.frame import RobotFrameYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.test_unit.utils import find_resources


def valid_robot_frame_cases() -> Iterable:
    yield pytest.param(
        {
            "name": "Robot Base Frame",
            "externalId": "robot_base_frame",
            "transform": {
                "parentFrameExternalId": "world_frame",
                "translation": {"x": 10.5, "y": 20.3, "z": 0.0},
                "orientation": {"x": 0.0, "y": 0.0, "z": 0.707, "w": 0.707},
            },
        },
        id="full-frame-with-transform",
    )
    yield pytest.param(
        {
            "name": "World Frame",
            "externalId": "world_frame",
        },
        id="minimal-robot-frame",
    )


def invalid_robot_frame_cases() -> Iterable:
    yield pytest.param(
        {"name": "Missing External ID"},
        {"Missing required field: 'externalId'"},
        id="missing-required-field",
    )
    yield pytest.param(
        {
            "name": "Test Frame",
            "externalId": "frame_001",
            "unknownField": "value",
        },
        {"Unused field: 'unknownField'"},
        id="unknown-field-present",
    )


class TestRobotFrameYAML:
    @pytest.mark.parametrize("data", list(find_resources("Frame", "robotics")))
    def test_load_valid_robot_frame_file(self, data: dict[str, object]) -> None:
        loaded = RobotFrameYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data", valid_robot_frame_cases())
    def test_load_valid_robot_frame(self, data: dict[str, object]) -> None:
        loaded = RobotFrameYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data, expected_errors", invalid_robot_frame_cases())
    def test_invalid_robot_frame_error_messages(self, data: dict | list, expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, RobotFrameYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)
        assert set(format_warning.errors) == expected_errors
