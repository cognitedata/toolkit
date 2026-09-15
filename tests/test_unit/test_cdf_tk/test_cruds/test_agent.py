from collections.abc import Mapping
from pathlib import Path
from typing import ClassVar
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.agent import AgentRequest, AgentResponse, SubagentConfig
from cognite_toolkit._cdf_tk.client.testing import ToolkitClientMock
from cognite_toolkit._cdf_tk.exceptions import ToolkitCycleError
from cognite_toolkit._cdf_tk.feature_flags import FeatureFlag, Flags
from cognite_toolkit._cdf_tk.resource_ios import DataModelIO, FunctionIO, SkillIO
from cognite_toolkit._cdf_tk.resource_ios._resource_ios.agent import AgentIO
from cognite_toolkit._cdf_tk.utils import calculate_hash
from cognite_toolkit._cdf_tk.utils.file import yaml_safe_dump
from cognite_toolkit._cdf_tk.yaml_classes import AgentYAML


class TestAgentIODumpResource:
    def test_dump_resource_ignores_empty_skills_when_omitted_locally(self) -> None:
        client = ToolkitClientMock()
        io = AgentIO(client, None, None)
        local = {
            "externalId": "my_agent",
            "name": "My Agent",
            "runtimeVersion": "0.9.9",
        }
        resource = AgentResponse.model_validate(
            {
                "externalId": "my_agent",
                "name": "My Agent",
                "createdTime": 0,
                "lastUpdatedTime": 0,
                "ownerId": "owner",
                "runtimeVersion": "0.9.9",
                "skills": [],
            }
        )

        dumped = io.dump_resource(resource, local)

        assert dumped == local

    def test_dump_resource_ignores_empty_subagents_when_omitted_locally(self) -> None:
        client = ToolkitClientMock()
        io = AgentIO(client, None, None)
        local = {
            "externalId": "my_agent",
            "name": "My Agent",
            "runtimeVersion": "1.3.0",
        }
        resource = AgentResponse.model_validate(
            {
                "externalId": "my_agent",
                "name": "My Agent",
                "createdTime": 0,
                "lastUpdatedTime": 0,
                "ownerId": "owner",
                "runtimeVersion": "1.3.0",
                "subagents": [],
            }
        )

        dumped = io.dump_resource(resource, local)

        assert dumped == local

    def test_dump_resource_ignores_empty_example_questions_when_omitted_locally(self) -> None:
        client = ToolkitClientMock()
        io = AgentIO(client, None, None)
        local = {
            "externalId": "my_agent",
            "name": "My Agent",
            "runtimeVersion": "0.9.9",
        }
        resource = AgentResponse.model_validate(
            {
                "externalId": "my_agent",
                "name": "My Agent",
                "createdTime": 0,
                "lastUpdatedTime": 0,
                "ownerId": "owner",
                "runtimeVersion": "0.9.9",
                "exampleQuestions": [],
            }
        )

        dumped = io.dump_resource(resource, local)

        assert dumped == local


class TestAgentIODependencies:
    def test_datamodel_is_in_class_dependencies(self) -> None:
        assert DataModelIO in AgentIO.dependencies

    def test_function_is_in_class_dependencies(self) -> None:
        assert FunctionIO in AgentIO.dependencies

    def test_get_dependencies_yields_subagent_references(self) -> None:
        resource = AgentYAML.model_validate(
            {
                "externalId": "supervisor",
                "name": "Supervisor",
                "runtimeVersion": "1.3.0",
                "subagents": [
                    {"agentExternalId": "weather-specialist"},
                    {"agentExternalId": "rca-specialist"},
                ],
            }
        )

        actual = list(AgentIO.get_dependencies(resource))

        assert actual == [
            (AgentIO, ExternalId(external_id="weather-specialist")),
            (AgentIO, ExternalId(external_id="rca-specialist")),
        ]

    def test_skill_is_in_class_dependencies(self) -> None:
        if FeatureFlag.is_enabled(Flags.AGENT_SKILLS):
            assert SkillIO in AgentIO.dependencies
        else:
            assert SkillIO not in AgentIO.dependencies


class TestAgentIODiffList:
    def test_diff_list_subagents_matches_by_agent_external_id(self) -> None:
        io = AgentIO(ToolkitClientMock(), None, None)
        local = [
            {"agentExternalId": "weather-specialist"},
            {"agentExternalId": "rca-specialist"},
        ]
        cdf = [
            {"agentExternalId": "weather-specialist"},
            {"agentExternalId": "rca-specialist"},
        ]

        local_by_cdf, added = io.diff_list(local, cdf, ("subagents",))

        assert local_by_cdf == {0: 0, 1: 1}
        assert added == []

    def test_diff_list_subagents_reports_cdf_only_subagents(self) -> None:
        io = AgentIO(ToolkitClientMock(), None, None)
        local = [{"agentExternalId": "weather-specialist"}]
        cdf = [
            {"agentExternalId": "weather-specialist"},
            {"agentExternalId": "rca-specialist"},
        ]

        local_by_cdf, added = io.diff_list(local, cdf, ("subagents",))

        assert local_by_cdf == {0: 0}
        assert added == [1]

    def test_diff_list_subagents_reports_no_match_when_external_ids_differ(self) -> None:
        io = AgentIO(ToolkitClientMock(), None, None)
        local = [{"agentExternalId": "weather-specialist"}]
        cdf = [{"agentExternalId": "rca-specialist"}]

        local_by_cdf, added = io.diff_list(local, cdf, ("subagents",))

        assert local_by_cdf == {}
        assert added == [0]

    def test_diff_list_example_questions_matches_by_question(self) -> None:
        io = AgentIO(ToolkitClientMock(), None, None)
        local = [
            {"question": "What can you do?"},
            {
                "question": "Can you show me all work orders concerning valves?",
                "expectedMessages": [{"role": "function", "content": "Finding maintenance orders..."}],
            },
        ]
        cdf = [
            {"question": "What can you do?"},
            {
                "question": "Can you show me all work orders concerning valves?",
                "expectedMessages": [{"role": "function", "content": "Finding maintenance orders..."}],
            },
        ]

        local_by_cdf, added = io.diff_list(local, cdf, ("exampleQuestions",))

        assert local_by_cdf == {0: 0, 1: 1}
        assert added == []

    def test_diff_list_example_questions_reports_cdf_only_questions(self) -> None:
        io = AgentIO(ToolkitClientMock(), None, None)
        local = [{"question": "What can you do?"}]
        cdf = [
            {"question": "What can you do?"},
            {"question": "Give a summary of the last shift and action points"},
        ]

        local_by_cdf, added = io.diff_list(local, cdf, ("exampleQuestions",))

        assert local_by_cdf == {0: 0}
        assert added == [1]


class TestAgentIOTopologicalSort:
    def test_topological_sort_orders_subagents_before_referencing_agent(self) -> None:
        supervisor = AgentRequest(
            external_id="supervisor",
            name="Supervisor",
            subagents=[SubagentConfig(agent_external_id="weather-specialist")],
        )
        subagent = AgentRequest(external_id="weather-specialist", name="Weather Specialist")

        actual = AgentIO.topological_sort([supervisor, subagent])

        assert [agent.external_id for agent in actual] == ["weather-specialist", "supervisor"]

    def test_topological_sort_raises_on_cycle(self) -> None:
        agent_a = AgentRequest(external_id="a", name="A", subagents=[SubagentConfig(agent_external_id="b")])
        agent_b = AgentRequest(external_id="b", name="B", subagents=[SubagentConfig(agent_external_id="a")])

        with pytest.raises(ToolkitCycleError):
            AgentIO.topological_sort([agent_a, agent_b])


class TestAgentIODelete:
    def test_delete_sorts_referencing_agent_before_subagent(self) -> None:
        supervisor = AgentResponse.model_validate(
            {
                "externalId": "supervisor",
                "name": "Supervisor",
                "createdTime": 0,
                "lastUpdatedTime": 0,
                "ownerId": "owner",
                "runtimeVersion": "1.3.0",
                "subagents": [{"agentExternalId": "weather-specialist"}],
            }
        )
        subagent = AgentResponse.model_validate(
            {
                "externalId": "weather-specialist",
                "name": "Weather Specialist",
                "createdTime": 0,
                "lastUpdatedTime": 0,
                "ownerId": "owner",
                "runtimeVersion": "1.3.0",
            }
        )
        client = ToolkitClientMock()
        client.tool.agents.retrieve.return_value = [supervisor, subagent]
        io = AgentIO(client, None, None)

        io.delete([ExternalId(external_id="weather-specialist"), ExternalId(external_id="supervisor")])

        deleted_ids = client.tool.agents.delete.call_args[0][0]
        assert deleted_ids == [
            ExternalId(external_id="supervisor"),
            ExternalId(external_id="weather-specialist"),
        ]


class TestAgentIOExtraFiles:
    _AGENT_YAML: ClassVar[Mapping[str, str]] = {
        "externalId": "my_agent",
        "name": "My Agent",
    }
    _TOOL: ClassVar[Mapping[str, str]] = {
        "type": "askDocument",
        "name": "Ask Document",
        "description": "Ask questions about documents in CDF.",
    }

    def test_get_extra_files(self, tmp_path: Path) -> None:
        markdown = "You are a helpful assistant.\n"
        docs_path = tmp_path / "instructions.md"
        docs_path.write_text(markdown, encoding="utf-8")

        tools_yaml = yaml_safe_dump([self._TOOL])
        tools_path = tmp_path / "tools.yaml"
        tools_path.write_text(tools_yaml, encoding="utf-8")

        python_code = "print('hello')\n"
        code_path = tmp_path / "run_code.py"
        code_path.write_text(python_code, encoding="utf-8")

        python_tool_yaml = yaml_safe_dump(
            {
                "type": "runPythonCode",
                "name": "run_code",
                "description": "A valid tool description for testing",
                "configuration": {"pythonCodeFile": "run_code.py"},
            }
        )
        python_tools_path = tmp_path / "python_tool.yaml"
        python_tools_path.write_text(python_tool_yaml, encoding="utf-8")

        yaml_path = MagicMock(spec=Path)
        yaml_path.parent = tmp_path

        extras = list(
            AgentIO.get_extra_files(
                yaml_path,
                ExternalId(external_id="my_agent"),
                {"instructionsFile": "instructions.md", "toolsFiles": ["tools.yaml", "python_tool.yaml"]},
            )
        )

        dumped = [e.model_dump(exclude_unset=True) for e in extras]
        assert dumped == [
            {
                "source_path": docs_path,
                "suffix": ".md",
                "content": markdown,
                "resource_field": "instructions",
                "source_hash": calculate_hash(markdown, shorten=True),
                "description": "agent instructions",
                "remove_fields": ["instructionsFile"],
            },
            {
                "source_path": tools_path,
                "suffix": ".yaml",
                "content": tools_yaml,
                "resource_field": "tools",
                "is_list": True,
                "source_hash": calculate_hash(tools_yaml, shorten=True),
                "description": "agent tools",
                "remove_fields": ["toolsFiles"],
            },
            {
                "source_path": code_path,
                "suffix": ".py",
                "content": python_code,
                "source_hash": calculate_hash(python_code, shorten=True),
                "description": "agent python code",
                "resource_field": None,
            },
            {
                "source_path": python_tools_path,
                "suffix": ".yaml",
                "content": python_tool_yaml,
                "resource_field": "tools",
                "is_list": True,
                "source_hash": calculate_hash(python_tool_yaml, shorten=True),
                "description": "agent tools",
                "remove_fields": ["toolsFiles"],
            },
        ]

    def test_get_extra_files_missing_instructions_file(self, tmp_path: Path) -> None:
        yaml_path = MagicMock(spec=Path)
        yaml_path.parent = tmp_path

        extras = list(
            AgentIO.get_extra_files(yaml_path, ExternalId(external_id="my_agent"), {"instructionsFile": "missing.md"})
        )

        assert len(extras) == 1
        extra = extras[0]
        assert extra.model_dump(exclude_unset=True)["code"] == "MISSING"

    def test_split_resource_writes_markdown_and_tools(
        self, tmp_path: Path, toolkit_client_cheap: ToolkitClient
    ) -> None:
        io = AgentIO(toolkit_client_cheap, None, None)
        base = tmp_path / "my_agent.Agent.yaml"
        python_code = "print('hello')\n"
        _python_tool = {
            "type": "runPythonCode",
            "name": "run_code",
            "description": "A valid tool description for testing",
        }
        resource = {
            **self._AGENT_YAML,
            "instructions": "Be helpful.\n",
            "tools": [
                self._TOOL,
                {
                    **_python_tool,
                    "configuration": {"pythonCode": python_code},
                },
            ],
        }

        out = list(io.split_resource(base, resource))

        assert out == [
            (base.with_suffix(".md"), "Be helpful.\n"),
            (tmp_path / "my_agent.Ask_Document.yaml", yaml_safe_dump(self._TOOL)),
            (tmp_path / "my_agent.run_code.py", python_code),
            (
                tmp_path / "my_agent.run_code.yaml",
                yaml_safe_dump({**_python_tool, "configuration": {"pythonCodeFile": "my_agent.run_code.py"}}),
            ),
            (
                base,
                {
                    **self._AGENT_YAML,
                    "instructionsFile": "my_agent.Agent.md",
                    "toolsFiles": ["my_agent.Ask_Document.yaml", "my_agent.run_code.yaml"],
                },
            ),
        ]
