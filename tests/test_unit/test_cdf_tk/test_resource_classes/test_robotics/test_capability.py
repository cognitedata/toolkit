from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes.robotics.capability import RobotCapabilityYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.test_unit.utils import find_resources


class TestRobotCapabilityYAML:
    @pytest.mark.parametrize("data", list(find_resources("RobotCapability")))
    def test_load_valid_robot_capability_file(self, data: dict[str, object]) -> None:
        loaded = RobotCapabilityYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize(
        "data",
        [
            pytest.param(
                {
                    "name": "PTZ Camera",
                    "externalId": "ptz_camera_001",
                    "method": "capture_ptz_image",
                    "description": "Pan-Tilt-Zoom camera capability for capturing images",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "pan": {"type": "number", "minimum": -180, "maximum": 180},
                            "tilt": {"type": "number", "minimum": -90, "maximum": 90},
                            "zoom": {"type": "number", "minimum": 1, "maximum": 10},
                        },
                    },
                    "dataHandlingSchema": {
                        "uploadLocation": "cdf_files",
                        "fileFormat": "jpeg",
                        "metadata": {"captureType": "ptz"},
                    },
                },
                id="full-ptz-camera-capability",
            ),
            pytest.param(
                {"name": "Docking Station", "externalId": "dock_001", "method": "return_to_dock"},
                id="minimal-robot-capability",
            ),
        ],
    )
    def test_load_valid_robot_capability(self, data: dict[str, object]) -> None:
        loaded = RobotCapabilityYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize(
        "data, expected_errors",
        [
            pytest.param(
                {"name": "Missing External ID", "method": "some_method"},
                {"Missing required field: 'externalId'"},
                id="missing-required-field",
            ),
            pytest.param(
                {"externalId": "cap_001", "name": "Test Capability", "method": "test_method", "unknownField": "value"},
                {"Unused field: 'unknownField'"},
                id="unknown-field-present",
            ),
        ],
    )
    def test_invalid_robot_capability_error_messages(self, data: dict | list, expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, RobotCapabilityYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)
        assert set(format_warning.errors) == expected_errors
