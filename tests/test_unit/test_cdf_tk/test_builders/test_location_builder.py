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
    with pytest.raises(ToolkitError, match=r"Circular dependency found in Locations*"):
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
    with pytest.raises(ToolkitError, match=r"Circular dependency found in Locations*"):
        list(location_builder.build(source_files=[source_file], module=module_location))
