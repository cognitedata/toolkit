from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes.node import NodeYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.test_unit.utils import find_resources


class TestNodeYAML:
    """Test suite for NodeYAML class."""

    @pytest.mark.parametrize("data", list(find_resources("Node")))
    def test_load_valid_node_from_resources(self, data: dict[str, object]) -> None:
        """Test loading valid nodes from resource files."""
        loaded = NodeYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    def test_node_with_minimal_fields(self) -> None:
        """Test node with only required fields."""
        minimal_data = {
            "space": "test_space",
            "externalId": "test_node",
        }
        loaded = NodeYAML.model_validate(minimal_data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == minimal_data

    def test_node_with_all_fields(self) -> None:
        """Test node with all fields populated."""
        full_data = {
            "space": "test_space",
            "externalId": "test_node",
            "type": {"space": "type_space", "externalId": "type_node"},
            "sources": [
                {
                    "source": {"type": "view", "space": "view_space", "externalId": "view_1", "version": "1"},
                    "properties": {"name": "Test Node", "value": 42, "tags": ["tag1", "tag2"]},
                }
            ],
            "existingVersion": 1,
        }
        loaded = NodeYAML.model_validate(full_data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == full_data

    def test_invalid_node_missing_required_fields(self) -> None:
        """Test that missing required fields are properly reported."""
        invalid_data = {
            "space": "my_space"
            # Missing 'externalId'
        }

        warning_list = validate_resource_yaml_pydantic(invalid_data, NodeYAML, Path("test.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)
        assert any("Missing required field: 'externalId'" in error for error in format_warning.errors)

    def test_node_with_multiple_sources(self) -> None:
        """Test node with multiple view sources."""
        data = {
            "space": "my_space",
            "externalId": "multi_source_node",
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
        loaded = NodeYAML.model_validate(data)
        assert len(loaded.sources) == 2
        assert isinstance(loaded.sources[0].properties, dict)
        assert "prop1" in loaded.sources[0].properties
        assert loaded.sources[0].properties["prop1"] == "value1"
        assert isinstance(loaded.sources[1].properties, dict)
        assert "prop2" in loaded.sources[1].properties
        assert loaded.sources[1].properties["prop2"] == "value2"
