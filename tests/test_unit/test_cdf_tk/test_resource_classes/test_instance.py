from collections.abc import Iterable
from datetime import datetime
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes.instance import EdgeYAML, NodeYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.test_unit.utils import find_resources

_EDGE_TEST_DATA = {
    "space": "test_space",
    "externalId": "edge_1",
    "type": {"space": "type_space", "externalId": "type_edge"},
    "startNode": {"space": "node_space", "externalId": "start_node"},
    "endNode": {"space": "node_space", "externalId": "end_node"},
}


def invalid_edge_test_cases() -> Iterable:
    yield pytest.param(
        {
            "existingVersion": 1,
        },
        {
            "Missing required field: 'externalId'",
            "Missing required field: 'space'",
            "Missing required field: 'type'",
            "Missing required field: 'startNode'",
            "Missing required field: 'endNode'",
        },
        id="missing-required-fields",
    )
    yield pytest.param(
        {
            **_EDGE_TEST_DATA,
            "existingVersion": -1,
        },
        {"In field existingVersion input should be greater than or equal to 0"},
        id="negative-existing-version",
    )
    yield pytest.param(
        {
            **_EDGE_TEST_DATA,
            "unknownField": "value",
        },
        {"Unused field: 'unknownField'"},
        id="unknown-field-present",
    )
    yield pytest.param(
        {
            **_EDGE_TEST_DATA,
            "space": "1invalid_space",
        },
        {"In field space string should match pattern '^[a-zA-Z][a-zA-Z0-9_-]{0,41}[a-zA-Z0-9]?$'"},
        id="invalid-space-pattern",
    )
    yield pytest.param(
        {
            **_EDGE_TEST_DATA,
            "space": "a" * 44,
        },
        {"In field space string should have at most 43 characters"},
        id="space-too-long",
    )
    yield pytest.param(
        {
            **_EDGE_TEST_DATA,
            "externalId": "",
        },
        {"In field externalId string should have at least 1 character"},
        id="invalid-external-id",
    )
    yield pytest.param(
        {
            **_EDGE_TEST_DATA,
            "externalId": "a" * 257,
        },
        {"In field externalId string should have at most 256 characters"},
        id="external-id-too-long",
    )
    yield pytest.param(
        {
            **_EDGE_TEST_DATA,
            "type": {
                "space": "type_space",
                "externalId": "type_edge",
                "unknownField": "value",
            },
        },
        {"In type unused field: 'unknownField'"},
        id="unknown-field-node-type",
    )
    yield pytest.param(
        {
            **_EDGE_TEST_DATA,
            "type": {
                "space": "",
                "externalId": "type_edge",
            },
        },
        {"In type.space string should have at least 1 character"},
        id="invalid-space-node-type",
    )
    yield pytest.param(
        {
            **_EDGE_TEST_DATA,
            "sources": [
                {
                    "source": {"type": "view", "space": "space1", "externalId": "view1", "version": "1"},
                    "properties": {23: "value1"},
                },
            ],
        },
        {
            "In sources[1].properties[24][key] input should be a valid string. Got 23 of type int. Hint: Use double quotes to force string."
        },
        id="invalid-properties-key",
    )
    yield pytest.param(
        {
            **_EDGE_TEST_DATA,
            "sources": [
                {
                    "source": {"type": "view", "space": "space1", "externalId": "view1", "version": "1"},
                    "properties": {"prop1": datetime.now()},
                },
            ],
        },
        {"In sources[1].properties.prop1 input was not a valid JSON value"},
        id="invalid-properties-value",
    )
    yield pytest.param(
        {
            **_EDGE_TEST_DATA,
            "sources": [
                {
                    "source": {"type": "unknown", "space": "space1", "externalId": "view1"},
                    "properties": {"prop1": "value"},
                },
            ],
        },
        {
            "In sources[1].source.ViewReference.type input should be 'view'. Got 'unknown'.",
            "In sources[1].source.ViewReference missing required field: 'version'",
            "In sources[1].source.ContainerReference.type input should be 'container'. Got 'unknown'.",
        },
        id="invalid-source-type",
    )


def valid_edge_test_cases() -> Iterable:
    yield pytest.param(
        _EDGE_TEST_DATA,
        id="minimal-valid-edge",
    )
    yield pytest.param(
        {
            **_EDGE_TEST_DATA,
            "sources": [
                {
                    "source": {"type": "view", "space": "view_space", "externalId": "view_1", "version": "1"},
                    "properties": {"name": "Test Edge", "weight": 1.23, "tags": ["tag1", "tag2"]},
                }
            ],
            "existingVersion": 1,
        },
        id="full-edge-with-all-fields",
    )
    yield pytest.param(
        {
            **_EDGE_TEST_DATA,
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
        },
        id="multi-source-edge",
    )


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
        assert set(format_warning.errors) == {"Missing required field: 'externalId'"}

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


class TestEdgeYAML:
    """Test suite for EdgeYAML class."""

    @pytest.mark.parametrize("data", list(find_resources("Edge")))
    def test_load_valid_edge_from_resources(self, data: dict[str, object]) -> None:
        """Test loading valid edges from resource files."""
        loaded = EdgeYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data", list(valid_edge_test_cases()))
    def test_valid_edges(self, data: dict[str, object]) -> None:
        """Test valid edge configurations."""
        loaded = EdgeYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data, expected_errors", list(invalid_edge_test_cases()))
    def test_invalid_edge_error_messages(self, data: dict | list, expected_errors: set[str]) -> None:
        """Test invalid edges with error messages."""
        warning_list = validate_resource_yaml_pydantic(data, EdgeYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)
        assert len(format_warning.errors) == len(expected_errors)
        assert set(format_warning.errors) == expected_errors
