from collections.abc import Iterable
from pathlib import Path
from typing import get_args

import pytest

from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses, literal_string_values_from_annotation
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from cognite_toolkit._cdf_tk.yaml_classes.agent import (
    EXAMPLE_QUESTIONS_MAX_LENGTH,
    EXAMPLE_QUESTIONS_MAX_SERIALIZED_SIZE,
    KNOWN_TOOLS,
    MAX_SUB_AGENTS_PER_AGENT,
    AgentInstanceSpaces,
    AgentInstanceSpacesDefinition,
    AgentTool,
    AgentToolDefinition,
    AgentYAML,
    Model,
)
from tests.data import COMPLETE_ORG_ALPHA_FLAGS
from tests.test_unit.utils import find_resources


def _model_literal_error_message(invalid_value: object) -> str:
    models = literal_string_values_from_annotation(Model)
    quoted = ", ".join(f"'{model}'" for model in models[:-1])
    options = f"{quoted} or '{models[-1]}'" if len(models) > 1 else f"'{models[0]}'"
    return f"In field model input should be {options}. Got {invalid_value!r}."


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
            _model_literal_error_message("invalid-model"),
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
            _model_literal_error_message(True),
            "In field name input should be a valid string. Got None of type NoneType. "
            "Hint: Use double quotes to force string.",
            "In field tools input should be a valid list. Got 'not_a_list'.",
            "Missing required field: 'externalId'",
            "Unknown field: 'external_id'",
        },
        id="type-validation-errors",
    )
    yield pytest.param(
        {"externalId": "valid_id", "name": "Valid Name", "tools": [{"type": "invalid"}]},
        {
            "In tools[1] input tag 'invalid' found using 'type' does not match any of the expected tags: 'analyzeImage', 'analyzeTimeSeries', 'askDocument', 'callFunction', 'callRestApi', 'examineDataSemantically', 'query', 'queryKnowledgeGraph', 'queryTimeSeriesDatapoints', 'runPythonCode', 'summarizeDocument', 'timeSeriesAnalysis'",
        },
        id="invalid-tool-type-validation-errors",
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

    def test_unknown_tool_type_is_validation_error(self) -> None:
        data = {
            "externalId": "my_agent",
            "name": "My Agent",
            "tools": [
                {"type": "unknownTool", "name": "Mystery", "description": "A valid tool description for testing"},
            ],
        }

        warning_list = validate_resource_yaml_pydantic(data, AgentYAML, Path("agent.yaml"))

        assert len(warning_list) == 1
        warning = warning_list[0]
        assert isinstance(warning, ResourceFormatWarning)
        assert any("unknownTool" in e for e in warning.errors)

    def test_tools_are_in_union(self) -> None:
        all_agent_tools = get_concrete_subclasses(AgentToolDefinition)
        all_union_agent_tools = get_args(AgentTool.__args__[0])
        missing = set(all_agent_tools) - set(all_union_agent_tools)
        assert not missing, (
            f"The following AgentTools subclasses are "
            f"missing from the AgentTool union: {humanize_collection([cls.__name__ for cls in missing])}"
        )

    @pytest.mark.parametrize(
        "tool_type",
        sorted(t for t in KNOWN_TOOLS if t not in {"callFunction", "query", "queryKnowledgeGraph"}),
    )
    def test_tool_extra_fields_roundtrip(self, tool_type: str) -> None:
        """Tools must preserve unknown fields so the API can add new properties without breaking deployments."""
        data = {
            "externalId": "my_agent",
            "name": "My Agent",
            "tools": [
                {
                    "type": tool_type,
                    "name": "my_tool",
                    "description": "A valid tool description for testing",
                    "someNewField": {"key": "value"},
                },
            ],
        }
        loaded = AgentYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    def test_instance_spaces_are_in_union(self) -> None:
        all_instance_spaces = get_concrete_subclasses(AgentInstanceSpacesDefinition)
        all_union_instance_spaces = get_args(AgentInstanceSpaces.__args__[0])
        missing = set(all_instance_spaces) - set(all_union_instance_spaces)
        assert not missing, (
            f"The following AgentInstanceSpaces subclasses are "
            f"missing from the AgentInstanceSpaces union: {humanize_collection([cls.__name__ for cls in missing])}"
        )

    @pytest.mark.parametrize(
        "tool",
        [
            pytest.param(
                {
                    "type": "query",
                    "name": "Query",
                    "description": "Run flexible queries against your data model and scope.",
                    "configuration": {
                        "dataModels": {
                            "type": "manual",
                            "dataModels": [
                                {
                                    "space": "cdf_cdm",
                                    "externalId": "CogniteCore",
                                    "version": "v1",
                                    "viewExternalIds": ["CogniteAsset"],
                                }
                            ],
                        },
                        "instanceSpaces": {"type": "all"},
                    },
                },
                id="manual-data-models-all-instance-spaces",
            ),
            pytest.param(
                {
                    "type": "query",
                    "name": "Query_Default",
                    "description": "Run flexible queries against your data model and scope.",
                    "configuration": {
                        "dataModels": {"type": "providedAtRuntime"},
                        "instanceSpaces": {"type": "providedAtRuntime"},
                    },
                },
                id="provided-at-runtime",
            ),
            pytest.param(
                {
                    "type": "query",
                    "name": "Query_manual_scope",
                    "description": "Run flexible queries against your data model and scope.",
                    "configuration": {
                        "dataModels": {"type": "providedAtRuntime"},
                        "instanceSpaces": {"type": "manual", "spaces": ["akerbp_wi"]},
                    },
                },
                id="runtime-data-models-manual-instance-spaces",
            ),
        ],
    )
    def test_query_tool_config_roundtrip(self, tool: dict) -> None:
        data = {"externalId": "my_agent", "name": "My Agent", "tools": [tool]}
        loaded = AgentYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    def test_subagents_roundtrip(self) -> None:
        data = {
            "externalId": "supervisor",
            "name": "Supervisor",
            "runtimeVersion": "1.3.0",
            "subagents": [
                {"agentExternalId": "weather-specialist"},
                {"agentExternalId": "rca-specialist"},
            ],
        }
        loaded = AgentYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize(
        "data, expected_error",
        [
            pytest.param(
                {
                    "externalId": "supervisor",
                    "name": "Supervisor",
                    "runtimeVersion": "1.3.0",
                    "subagents": [
                        {"agentExternalId": "weather-specialist"},
                        {"agentExternalId": "weather-specialist"},
                    ],
                },
                "duplicate subagent agentExternalId(s): ['weather-specialist']. Each entry must reference a distinct agent.",
                id="duplicate-subagents",
            ),
            pytest.param(
                {
                    "externalId": "supervisor",
                    "name": "Supervisor",
                    "runtimeVersion": "1.3.0",
                    "subagents": [{"agentExternalId": ""}],
                },
                "In subagents[1].agentExternalId string should have at least 1 character",
                id="empty-subagent-external-id",
            ),
            pytest.param(
                {
                    "externalId": "supervisor",
                    "name": "Supervisor",
                    "runtimeVersion": "1.0.0",
                    "subagents": [{"agentExternalId": "weather-specialist"}],
                },
                "Runtime version '1.0.0' does not support subagents. "
                "Use a runtime version where supports_subagents is enabled, or remove the 'subagents' field.",
                id="unsupported-runtime-version",
            ),
            pytest.param(
                {
                    "externalId": "supervisor",
                    "name": "Supervisor",
                    "runtimeVersion": "1.3.0",
                    "subagents": [{"agentExternalId": "supervisor"}],
                },
                "An agent cannot reference itself as a subagent.",
                id="self-reference",
            ),
            pytest.param(
                {
                    "externalId": "supervisor",
                    "name": "Supervisor",
                    "runtimeVersion": "1.3.0",
                    "tools": [
                        {
                            "type": "askDocument",
                            "name": "delegate_to_subagent",
                            "description": "A valid tool description for testing",
                        }
                    ],
                    "subagents": [{"agentExternalId": "weather-specialist"}],
                },
                "Tool name 'delegate_to_subagent' is reserved for the system sub-agent delegate tool. Rename the tool.",
                id="reserved-delegate-tool-name",
            ),
        ],
    )
    def test_subagents_validation_errors(self, data: dict, expected_error: str) -> None:
        warning_list = validate_resource_yaml_pydantic(data, AgentYAML, Path("agent.yaml"))
        assert len(warning_list) == 1
        warning = warning_list[0]
        assert isinstance(warning, ResourceFormatWarning)
        assert any(expected_error in error for error in warning.errors)

    def test_subagents_max_length_validation(self) -> None:
        data = {
            "externalId": "supervisor",
            "name": "Supervisor",
            "runtimeVersion": "1.3.0",
            "subagents": [{"agentExternalId": f"agent-{index}"} for index in range(MAX_SUB_AGENTS_PER_AGENT + 1)],
        }
        warning_list = validate_resource_yaml_pydantic(data, AgentYAML, Path("agent.yaml"))
        assert len(warning_list) == 1
        warning = warning_list[0]
        assert isinstance(warning, ResourceFormatWarning)
        assert any(f"list should have at most {MAX_SUB_AGENTS_PER_AGENT} items" in error for error in warning.errors)

    def test_example_questions_roundtrip_question_only(self) -> None:
        data = {
            "externalId": "my_agent",
            "name": "My Agent",
            "exampleQuestions": [
                {"question": "What can you do?"},
                {"question": "Give a summary of the last shift and action points"},
            ],
        }
        loaded = AgentYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    def test_example_questions_roundtrip_with_expected_messages(self) -> None:
        data = {
            "externalId": "my_agent",
            "name": "My Agent",
            "exampleQuestions": [
                {
                    "question": "Can you show me all work orders concerning valves?",
                    "expectedMessages": [
                        {"role": "function", "content": "Finding maintenance orders..."},
                    ],
                },
                {
                    "question": "List all raw databases",
                    "expectedMessages": [
                        {"role": "function", "content": "Calling Rest Api..."},
                        {"role": "function", "content": "Fetching results..."},
                    ],
                },
            ],
        }
        loaded = AgentYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    def test_example_questions_empty_list_roundtrip(self) -> None:
        data = {
            "externalId": "my_agent",
            "name": "My Agent",
            "exampleQuestions": [],
        }
        loaded = AgentYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize(
        "data, expected_error",
        [
            pytest.param(
                {
                    "externalId": "my_agent",
                    "name": "My Agent",
                    "exampleQuestions": ["What can you do?"],
                },
                "In exampleQuestions[1] input must be an object of type ExampleQuestion",
                id="bare-string-shorthand",
            ),
            pytest.param(
                {
                    "externalId": "my_agent",
                    "name": "My Agent",
                    "exampleQuestions": [{}],
                },
                "In exampleQuestions[1] missing required field: 'question'",
                id="missing-question",
            ),
            pytest.param(
                {
                    "externalId": "my_agent",
                    "name": "My Agent",
                    "exampleQuestions": [{"question": ""}],
                },
                "In exampleQuestions[1].question string should have at least 1 character",
                id="empty-question",
            ),
            pytest.param(
                {
                    "externalId": "my_agent",
                    "name": "My Agent",
                    "exampleQuestions": [
                        {
                            "question": "Can you show me all work orders concerning valves?",
                            "expectedMessages": [{"role": "function"}],
                        }
                    ],
                },
                "In exampleQuestions[1].expectedMessages[1] missing required field: 'content'",
                id="expected-message-missing-content",
            ),
            pytest.param(
                {
                    "externalId": "my_agent",
                    "name": "My Agent",
                    "exampleQuestions": [
                        {
                            "question": "Can you show me all work orders concerning valves?",
                            "expectedMessages": [{"content": "Finding maintenance orders..."}],
                        }
                    ],
                },
                "In exampleQuestions[1].expectedMessages[1] missing required field: 'role'",
                id="expected-message-missing-role",
            ),
        ],
    )
    def test_example_questions_validation_errors(self, data: dict, expected_error: str) -> None:
        warning_list = validate_resource_yaml_pydantic(data, AgentYAML, Path("agent.yaml"))
        assert len(warning_list) == 1
        warning = warning_list[0]
        assert isinstance(warning, ResourceFormatWarning)
        assert any(expected_error in error for error in warning.errors)

    def test_example_questions_max_length_validation(self) -> None:
        data = {
            "externalId": "my_agent",
            "name": "My Agent",
            "exampleQuestions": [{"question": f"Question {index}"} for index in range(EXAMPLE_QUESTIONS_MAX_LENGTH + 1)],
        }
        warning_list = validate_resource_yaml_pydantic(data, AgentYAML, Path("agent.yaml"))
        assert len(warning_list) == 1
        warning = warning_list[0]
        assert isinstance(warning, ResourceFormatWarning)
        assert any(f"list should have at most {EXAMPLE_QUESTIONS_MAX_LENGTH} items" in error for error in warning.errors)

    def test_example_questions_serialized_size_validation(self) -> None:
        oversized_question = "x" * (EXAMPLE_QUESTIONS_MAX_SERIALIZED_SIZE - len('{"questions":[{"question":""}]}') + 1)
        data = {
            "externalId": "my_agent",
            "name": "My Agent",
            "exampleQuestions": [{"question": oversized_question}],
        }
        warning_list = validate_resource_yaml_pydantic(data, AgentYAML, Path("agent.yaml"))
        assert len(warning_list) == 1
        warning = warning_list[0]
        assert isinstance(warning, ResourceFormatWarning)
        assert any(
            f"exceeds the maximum of {EXAMPLE_QUESTIONS_MAX_SERIALIZED_SIZE} bytes" in error for error in warning.errors
        )
