from collections.abc import Iterable
from pathlib import Path
from typing import get_args

import pytest

from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.resource_classes.agent import AgentYAML
from cognite_toolkit._cdf_tk.resource_classes.agent_tools import (
    AgentInstanceSpaces,
    AgentInstanceSpacesDefinition,
    AgentTool,
    AgentToolDefinition,
)
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.data import COMPLETE_ORG_ALPHA_FLAGS
from tests.test_unit.utils import find_resources


def invalid_test_cases() -> Iterable:
    yield pytest.param(
        {
            "externalId": "my_space",
        },
        {"Missing required field: 'name'"},
        id="missing-required-field",
    )
    yield pytest.param(
        {
            "externalId": "",  # Empty string - violates min_length=1
            "name": "",  # Empty string - violates min_length=1
            "description": "a" * 1025,  # Too long - violates max_length=1024
            "model": "invalid-model",  # Not in Model literal enum
            "tools": [{"type": "invalid"}] * 21,  # Too many tools - violates max_length=20
        },
        {
            "In field description string should have at most 1024 characters",
            "In field externalId string should have at least 1 character",
            "In field model input should be 'azure/o3', 'azure/o4-mini', 'azure/gpt-4o', "
            "'azure/gpt-4o-mini', 'azure/gpt-4.1', 'azure/gpt-4.1-nano', "
            "'azure/gpt-4.1-mini', 'azure/gpt-5', 'azure/gpt-5-mini', 'azure/gpt-5-nano', "
            "'gcp/gemini-2.5-pro', 'gcp/gemini-2.5-flash', 'aws/claude-4-sonnet', "
            "'aws/claude-4-opus', 'aws/claude-4.1-opus' or 'aws/claude-3.5-sonnet'. Got "
            "'invalid-model'.",
            "In field name string should have at least 1 character",
            "In field tools list should have at most 20 items after validation, not 21",
        },
        id="multiple-validation-errors",
    )
    yield pytest.param(
        {
            "externalId": "a" * 129,  # Too long - violates max_length=128
            "name": "a" * 256,  # Too long - violates max_length=255
            "instructions": "a" * 32001,  # Too long - violates max_length=32000
            "tools": [
                {
                    "type": "askDocument",
                    "name": "",  # Empty name - violates min_length=1
                    "description": "short",  # Too short - violates min_length=10
                }
            ],
        },
        {
            "In field externalId string should have at most 128 characters",
            "In field instructions string should have at most 32000 characters",
            "In field name string should have at most 255 characters",
            "In tools[1].askDocument.description string should have at least 10 characters",
            "In tools[1].askDocument.name string should have at least 1 character",
        },
        id="length-validation-errors",
    )
    yield pytest.param(
        {
            "externalId": "valid_id",
            "name": "Valid Name",
            "tools": [
                {
                    "type": "queryKnowledgeGraph",
                    "name": "test_tool",
                    "description": "This is a valid description for the tool",
                    "configuration": {
                        "dataModels": [],  # Empty list - violates min_length=1
                        "instanceSpaces": {
                            "type": "manual",
                            # Missing required 'spaces' field for manual type
                        },
                        "version": "v3",  # Invalid literal - not in ["v1", "v2"]
                    },
                }
            ],
        },
        {
            "In tools[1].queryKnowledgeGraph.configuration.dataModels list should have at "
            "least 1 item after validation, not 0",
            "In tools[1].queryKnowledgeGraph.configuration.instanceSpaces.manual missing required field: 'spaces'",
            "In tools[1].queryKnowledgeGraph.configuration.version input should be 'v1' or 'v2'. Got 'v3'.",
        },
        id="nested-tool-validation-errors",
    )
    yield pytest.param(
        {
            "externalId": "\x00invalid",  # Contains null character - violates pattern
            "name": "Valid Name",
            "tools": [
                {
                    "type": "queryKnowledgeGraph",
                    "name": "\x00tool",  # Contains null character - violates pattern
                    "description": "a" * 1025,  # Too long - violates max_length=1024
                    "configuration": {
                        "dataModels": [
                            {
                                "space": "invalid space!",  # Invalid pattern - contains space and special char
                                "external_id": "123invalid",  # Invalid pattern - starts with number
                                "version": "v@1",  # Invalid pattern - contains special char
                                "viewExternalIds": ["valid_view"] * 11,  # Too many items - violates max_length=10
                            }
                        ]
                        * 81,  # Too many data models - violates max_length=80
                        "instanceSpaces": {"type": "all"},
                    },
                }
            ],
        },
        {
            "In field externalId string should match pattern '^[^\\x00]{1,128}$'",
            "In tools[1].queryKnowledgeGraph.configuration.dataModels list should have at "
            "most 80 items after validation, not 81",
            "In tools[1].queryKnowledgeGraph.description string should have at most 1024 characters",
            "In tools[1].queryKnowledgeGraph.name string should match pattern '^[^\\x00]{1,64}$'",
        },
        id="pattern-and-nested-validation-errors",
    )
    yield pytest.param(
        {
            "external_id": 123,  # Wrong type - should be string
            "name": None,  # Wrong type - should be string
            "description": 456,  # Wrong type - should be string or None
            "instructions": [],  # Wrong type - should be string or None
            "model": True,  # Wrong type - should be Model literal
            "tools": "not_a_list",  # Wrong type - should be list
        },
        {
            "In field description input should be a valid string. Got 456 of type int. "
            "Hint: Use double quotes to force string.",
            "In field instructions input should be a valid string. Got [] of type list. "
            "Hint: Use double quotes to force string.",
            "In field model input should be 'azure/o3', 'azure/o4-mini', 'azure/gpt-4o', "
            "'azure/gpt-4o-mini', 'azure/gpt-4.1', 'azure/gpt-4.1-nano', "
            "'azure/gpt-4.1-mini', 'azure/gpt-5', 'azure/gpt-5-mini', 'azure/gpt-5-nano', "
            "'gcp/gemini-2.5-pro', 'gcp/gemini-2.5-flash', 'aws/claude-4-sonnet', "
            "'aws/claude-4-opus', 'aws/claude-4.1-opus' or 'aws/claude-3.5-sonnet'. Got "
            "True.",
            "In field name input should be a valid string. Got None of type NoneType. "
            "Hint: Use double quotes to force string.",
            "In field tools input should be a valid list. Got 'not_a_list'.",
            "Missing required field: 'externalId'",
            "Unused field: 'external_id'",
        },
        id="type-validation-errors",
    )


class TestAgentYAML:
    @pytest.mark.parametrize("data", list(find_resources("Agent", base=COMPLETE_ORG_ALPHA_FLAGS / MODULES)))
    def test_load_valid(self, data: dict[str, object]) -> None:
        loaded = AgentYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data, expected_errors", list(invalid_test_cases()))
    def test_invalid_model_error_messages(self, data: dict | list, expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, AgentYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert set(format_warning.errors) == expected_errors

    def test_tools_are_in_union(self) -> None:
        all_agent_tools = get_concrete_subclasses(AgentToolDefinition)
        all_union_agent_tools = get_args(AgentTool.__args__[0])
        missing = set(all_agent_tools) - set(all_union_agent_tools)
        assert not missing, (
            f"The following AgentTools subclasses are "
            f"missing from the AgentTool union: {humanize_collection([cls.__name__ for cls in missing])}"
        )

    def test_instance_spaces_are_in_union(self) -> None:
        all_instance_spaces = get_concrete_subclasses(AgentInstanceSpacesDefinition)
        all_union_instance_spaces = get_args(AgentInstanceSpaces.__args__[0])
        missing = set(all_instance_spaces) - set(all_union_instance_spaces)
        assert not missing, (
            f"The following AgentInstanceSpaces subclasses are "
            f"missing from the AgentInstanceSpaces union: {humanize_collection([cls.__name__ for cls in missing])}"
        )
