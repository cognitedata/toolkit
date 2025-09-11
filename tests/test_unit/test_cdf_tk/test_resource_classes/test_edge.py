from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes import EdgeYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.test_unit.utils import find_resources


class TestEdgeYAML:
    """Test suite for EdgeYAML class."""

    @pytest.mark.parametrize("data", list(find_resources("Edge")))
    def test_load_valid_edge_from_resources(self, data: dict[str, object]) -> None:
        """Test loading valid edges from resource files."""
        loaded = EdgeYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    def test_edge_with_minimal_fields(self) -> None:
        """Test edge with only required fields."""
        minimal_data = {
            "space": "test_space",
            "externalId": "test_edge",
            "type": {"space": "type_space", "externalId": "type_edge"},
            "startNode": {"space": "node_space", "externalId": "start_node"},
            "endNode": {"space": "node_space", "externalId": "end_node"},
        }
        loaded = EdgeYAML.model_validate(minimal_data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == minimal_data

    def test_edge_with_all_fields(self) -> None:
        """Test edge with all fields populated."""
        full_data = {
            "space": "test_space",
            "externalId": "test_edge",
            "type": {"space": "type_space", "externalId": "type_edge"},
            "sources": [
                {
                    "source": {"type": "view", "space": "view_space", "externalId": "view_1", "version": "1"},
                    "properties": {"name": "Test Edge", "weight": 1.23, "tags": ["tag1", "tag2"]},
                }
            ],
            "existingVersion": 1,
            "startNode": {"space": "node_space", "externalId": "start_node"},
            "endNode": {"space": "node_space", "externalId": "end_node"},
        }
        loaded = EdgeYAML.model_validate(full_data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == full_data

    def test_invalid_edge_missing_required_fields(self) -> None:
        """Test that missing required fields are properly reported."""
        invalid_data = {
            "space": "my_space",
            "type": {"space": "type_space", "externalId": "type_edge"},
            "startNode": {"space": "node_space", "externalId": "start_node"},
            "endNode": {"space": "node_space", "externalId": "end_node"},
            # Missing 'externalId'
        }

        warning_list = validate_resource_yaml_pydantic(invalid_data, EdgeYAML, Path("test.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)
        assert any("Missing required field: 'externalId'" in error for error in format_warning.errors)

    def test_edge_with_multiple_sources(self) -> None:
        """Test edge with multiple view sources."""
        data = {
            "space": "my_space",
            "externalId": "multi_source_edge",
            "type": {"space": "type_space", "externalId": "type_edge"},
            "startNode": {"space": "node_space", "externalId": "start_node"},
            "endNode": {"space": "node_space", "externalId": "end_node"},
            "sources": [
                {
                    "source": {"type": "view", "space": "space1", "externalId": "view1", "version": "1"},
                    "properties": {"prop1": "value1"},
                },
                {
                    "source": {"type": "view", "space": "space2", "externalId": "view2", "version": "1"},
                    "properties": {"prop2": "value2"},
                },
            ],
        }
        loaded = EdgeYAML.model_validate(data)
        assert len(loaded.sources) == 2
        assert isinstance(loaded.sources[0].properties, dict)
        assert "prop1" in loaded.sources[0].properties
        assert loaded.sources[0].properties["prop1"] == "value1"
        assert isinstance(loaded.sources[1].properties, dict)
        assert "prop2" in loaded.sources[1].properties
        assert loaded.sources[1].properties["prop2"] == "value2"
