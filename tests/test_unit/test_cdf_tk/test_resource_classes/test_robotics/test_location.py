from collections.abc import Iterable
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes.robotics.location import RobotLocationYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.test_unit.utils import find_resources


def valid_robot_location_cases() -> Iterable:
    yield pytest.param(
        {
            "name": "Factory Floor A",
            "externalId": "factory_floor_a",
            "description": "Main production floor with assembly lines 1-5 and robot inspection stations",
        },
        id="full-location-with-description",
    )
    yield pytest.param(
        {
            "name": "Warehouse Section B",
            "externalId": "warehouse_b",
        },
        id="minimal-robot-location",
    )


def invalid_robot_location_cases() -> Iterable:
    yield pytest.param(
        {"name": "Missing External ID"},
        {"Missing required field: 'externalId'"},
        id="missing-required-field-external-id",
    )
    yield pytest.param(
        {
            "name": "Test Location",
            "externalId": "loc_001",
            "unknownField": "value",
        },
        {"Unused field: 'unknownField'"},
        id="unknown-field-present",
    )


class TestRobotLocationYAML:
    @pytest.mark.parametrize("data", list(find_resources("Location", "robotics")))
    def test_load_valid_robot_location_file(self, data: dict[str, object]) -> None:
        loaded = RobotLocationYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data", valid_robot_location_cases())
    def test_load_valid_robot_location(self, data: dict[str, object]) -> None:
        loaded = RobotLocationYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data, expected_errors", invalid_robot_location_cases())
    def test_invalid_robot_location_error_messages(self, data: dict | list, expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, RobotLocationYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)
        assert set(format_warning.errors) == expected_errors
