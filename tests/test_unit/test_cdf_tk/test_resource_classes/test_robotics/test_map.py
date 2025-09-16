from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes.robotics.map import RobotMapYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.test_unit.utils import find_resources


class TestRobotMapYAML:
    @pytest.mark.parametrize("data", list(find_resources("Map", "robotics")))
    def test_load_valid_robot_map_file(self, data: dict[str, object]) -> None:
        loaded = RobotMapYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize(
        "data",
        [
            {"name": "Warehouse Map", "externalId": "map_001", "mapType": "WAYPOINTMAP"},
            {
                "name": "3D Model Map",
                "externalId": "map_3d_001",
                "mapType": "THREEDMODEL",
                "description": "3D model of the facility",
                "frameExternalId": "frame_001",
                "data": {"modelUrl": "https://example.com/model.obj"},
                "locationExternalId": "location_001",
                "scale": 1.0,
            },
        ],
    )
    def test_load_valid_map(self, data: dict[str, object]) -> None:
        """Test valid map configurations"""
        loaded = RobotMapYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize(
        "data,expected_error",
        [
            ({"name": "Missing Map Type", "externalId": "map_001"}, {"Missing required field: 'mapType'"}),
            (
                {"name": "Invalid Map", "externalId": "map_invalid", "mapType": "INVALID_TYPE"},
                {
                    "In field mapType input should be 'WAYPOINTMAP', 'THREEDMODEL', 'TWODMAP' or 'POINTCLOUD'. Got 'INVALID_TYPE'."
                },
            ),
            (
                {
                    "name": "3D Model Map",
                    "externalId": "map_3d_001",
                    "mapType": "THREEDMODEL",
                    "description": "3D model of the facility",
                    "frameExternalId": "frame_001",
                    "data": {"modelUrl": "https://example.com/model.obj"},
                    "locationExternalId": "location_001",
                    "scale": 1.1,
                },
                {"In field scale input should be less than or equal to 1"},
            ),
        ],
    )
    def test_invalid_map_configurations(self, data: dict[str, object], expected_error: str) -> None:
        """Test invalid map configurations"""
        warning_list = validate_resource_yaml_pydantic(data, RobotMapYAML, Path("test.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)
        assert set(format_warning.errors) == expected_error
