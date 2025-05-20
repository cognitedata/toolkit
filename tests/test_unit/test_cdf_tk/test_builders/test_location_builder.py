from pathlib import Path
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.builders._location import LocationBuilder
from cognite_toolkit._cdf_tk.data_classes._build_files import BuildSourceFile
from cognite_toolkit._cdf_tk.data_classes._module_directories import ModuleLocation
from cognite_toolkit._cdf_tk.exceptions import ToolkitError


def test_location_builder_detect_cyclic_references(build_tmp_path):
    # Setup

    module_location = MagicMock(spec=ModuleLocation)

    source_file = MagicMock(spec=BuildSourceFile)
    source_file.source = MagicMock()
    source_file.source.path = Path("cyclic.LocationFilter.yaml")
    source_file.loaded = [
        {"externalId": "location1", "description": "Location 1", "parentExternalId": "location2"},
        {"externalId": "location2", "description": "Location 2", "parentExternalId": "location3"},
        {
            "externalId": "location3",
            "description": "Location 3",
            "parentExternalId": "location1",  # This creates a cycle
        },
    ]

    location_builder = LocationBuilder(build_dir=build_tmp_path)
    with pytest.raises(ToolkitError, match="Circular dependency found in Locations*"):
        list(location_builder.build(source_files=[source_file], module=module_location))


def test_location_builder_detect_self_reference(build_tmp_path):
    # Setup

    module_location = MagicMock(spec=ModuleLocation)

    source_file = MagicMock(spec=BuildSourceFile)
    source_file.source = MagicMock()
    source_file.source.path = Path("self.LocationFilter.yaml")
    source_file.loaded = [
        {
            "externalId": "location1",
            "description": "Location 1",
            "parentExternalId": "location1",
        },  # This creates a self-reference
    ]

    location_builder = LocationBuilder(build_dir=build_tmp_path)
    with pytest.raises(ToolkitError, match="Circular dependency found in Locations*"):
        list(location_builder.build(source_files=[source_file], module=module_location))


@pytest.mark.parametrize(
    "filename_given_by_user",
    [Path("originally_multiple_locations.LocationFilter.yaml"), Path("originally_multiple_locations.yaml")],
)
def test_test_sequenced_location_files(build_tmp_path, filename_given_by_user):
    location_builder = LocationBuilder(build_dir=build_tmp_path)

    destination_paths = [
        location_builder._create_file_path(filename_given_by_user, i, "LocationFilter") for i in range(1, 4)
    ]

    destination_dir = build_tmp_path / location_builder.resource_folder

    assert len(destination_paths) == 3
    assert destination_paths[0] == destination_dir / "1.originally_multiple_locations.LocationFilter.yaml"
    assert destination_paths[1] == destination_dir / "2.originally_multiple_locations.LocationFilter.yaml"
    assert destination_paths[2] == destination_dir / "3.originally_multiple_locations.LocationFilter.yaml"
