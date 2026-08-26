from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.agent import (
    AgentRuntimeVersionInfo,
    AIServiceAvailability,
    ServicesAvailability,
)
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuiltResource
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import ConsistencyError
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import ResourceType
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._types import AbsoluteFilePath
from cognite_toolkit._cdf_tk.resource_ios import AgentIO
from cognite_toolkit._cdf_tk.rules._agents import AgentRules


class TestAgentRules:
    """Test suite for AgentRules validation."""

    @pytest.fixture
    def service_availability(self) -> ServicesAvailability:
        """Create a sample ServicesAvailability object for testing."""
        return ServicesAvailability(
            items=[
                AIServiceAvailability(
                    name="Agent CRUD",
                    path="/ai/agents",
                    available=True,
                    supported_language_models=["azure/gpt-4.1", "gcp/claude-5-sonnet"],
                    additional_parameters={"maxToolsPerAgentLimit": 2},
                    default_runtime_version="1.0.0",
                    agent_runtime_versions=[
                        AgentRuntimeVersionInfo(version="1.0.0", release_stage="stable", capabilities=[]),
                        AgentRuntimeVersionInfo(version="1.3.0", release_stage="preview", capabilities=["SUBAGENTS"]),
                    ],
                )
            ],
            language_models=[],
        )

    @staticmethod
    def _write_agent_yaml(filepath: Path, content: dict) -> None:
        """Helper to write agent YAML content."""
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w") as f:
            yaml.safe_dump(content, f)

    @staticmethod
    def _create_built_resource(source_path: Path, build_path: Path, external_id: str = "my_agent") -> BuiltResource:
        """Create a BuiltResource for testing."""
        return BuiltResource(
            identifier=ExternalId(external_id=external_id),
            source_hash="test-hash",
            type=ResourceType(resource_folder=AgentIO.folder_name, kind=AgentIO.kind),
            source_path=AbsoluteFilePath(source_path.resolve()),
            build_path=AbsoluteFilePath(build_path.resolve()),
            crud_cls=AgentIO,
            dependencies=set(),
            has_syntax_error=False,
        )

    @staticmethod
    def _create_rule_with_client(service_availability: ServicesAvailability) -> AgentRules:
        """Create an AgentRules with a mocked client."""
        mock_client = MagicMock()
        mock_client.tool.agents.service_availability.return_value = service_availability
        return AgentRules(modules=[], client=mock_client)

    @pytest.mark.parametrize(
        "with_client, expected_code, expected_message_part",
        [
            pytest.param(False, "reduced", "requires a client", id="no-client"),
            pytest.param(True, "ready", "validate agent models", id="with-client"),
        ],
    )
    def test_get_status(
        self,
        service_availability: ServicesAvailability,
        with_client: bool,
        expected_code: str,
        expected_message_part: str,
    ) -> None:
        rule = self._create_rule_with_client(service_availability) if with_client else AgentRules(modules=[])
        status = rule.get_status()
        assert status.code == expected_code
        assert expected_message_part in status.message.lower()

    def test_service_availability_returns_none_without_client(self) -> None:
        rule = AgentRules(modules=[])
        assert rule.service_availability is None

    @pytest.mark.parametrize(
        "model, with_client, expected_codes",
        [
            pytest.param("gcp/claude-5-opus", True, ["AGENT-MODEL"], id="unknown-model"),
            pytest.param("azure/gpt-4.1", True, [], id="known-model"),
            pytest.param(None, True, [], id="unset-model-is-allowed"),
            pytest.param("some-brand-new-model", False, [], id="no-client-allows-any-model"),
        ],
    )
    def test_validate_agent_model(
        self,
        tmp_path: Path,
        service_availability: ServicesAvailability,
        model: str | None,
        with_client: bool,
        expected_codes: list[str],
    ) -> None:
        content = {"externalId": "my_agent", "name": "My Agent"}
        if model is not None:
            content["model"] = model
        yaml_file = tmp_path / "agents" / "agent.yaml"
        self._write_agent_yaml(yaml_file, content)
        resource = self._create_built_resource(yaml_file, yaml_file)
        rule = self._create_rule_with_client(service_availability) if with_client else AgentRules(modules=[])
        errors = list(rule._validate_agent(resource))
        assert [error.code for error in errors] == expected_codes
        assert all(isinstance(error, ConsistencyError) for error in errors)

    def test_validate_agent_too_many_tools(self, tmp_path: Path, service_availability: ServicesAvailability) -> None:
        yaml_file = tmp_path / "agents" / "agent.yaml"
        self._write_agent_yaml(
            yaml_file,
            {
                "externalId": "my_agent",
                "name": "My Agent",
                "model": "azure/gpt-4.1",
                "tools": [
                    {"type": "askDocument", "name": "tool_one", "description": "A valid tool description"},
                    {"type": "askDocument", "name": "tool_two", "description": "A valid tool description"},
                    {"type": "askDocument", "name": "tool_three", "description": "A valid tool description"},
                ],
            },
        )
        resource = self._create_built_resource(yaml_file, yaml_file)
        rule = self._create_rule_with_client(service_availability)
        errors = list(rule._validate_agent(resource))
        assert len(errors) == 1
        assert errors[0].code == "AGENT-TOOLS-LIMIT"

    @pytest.mark.parametrize(
        "runtime_version, extra_fields, with_client, expected_codes",
        [
            pytest.param("1.0.0", {}, True, [], id="known-runtime-version-no-gated-fields"),
            pytest.param("9.9.9", {}, True, ["AGENT-UNKNOWN-RUNTIME"], id="unknown-runtime-version"),
            pytest.param("9.9.9", {}, False, [], id="no-client-allows-any-runtime-version"),
            pytest.param(
                "1.0.0",
                {"subagents": [{"agentExternalId": "specialist"}]},
                True,
                ["AGENT-RUNTIME-UNSUPPORTED-CAPABILITY"],
                id="subagents-unsupported-runtime-version",
            ),
            pytest.param(
                "1.3.0",
                {"subagents": [{"agentExternalId": "specialist"}]},
                True,
                [],
                id="subagents-supported-runtime-version",
            ),
            pytest.param(
                "1.0.0",
                {"skills": ["my_skill"]},
                True,
                ["AGENT-RUNTIME-UNSUPPORTED-CAPABILITY"],
                id="skills-unsupported-runtime-version",
            ),
            pytest.param(
                None,
                {"subagents": [{"agentExternalId": "specialist"}]},
                True,
                ["AGENT-RUNTIME-UNSUPPORTED-CAPABILITY"],
                id="unset-runtime-version-falls-back-to-unsupported-default",
            ),
            pytest.param(
                None,
                {},
                True,
                [],
                id="unset-runtime-version-no-gated-fields",
            ),
        ],
    )
    def test_validate_agent_runtime_version_and_capabilities(
        self,
        tmp_path: Path,
        service_availability: ServicesAvailability,
        runtime_version: str | None,
        extra_fields: dict,
        with_client: bool,
        expected_codes: list[str],
    ) -> None:
        yaml_file = tmp_path / "agents" / "agent.yaml"
        self._write_agent_yaml(
            yaml_file,
            {
                "externalId": "supervisor",
                "name": "Supervisor",
                "model": "azure/gpt-4.1",
                "runtimeVersion": runtime_version,
                **extra_fields,
            },
        )
        resource = self._create_built_resource(yaml_file, yaml_file, external_id="supervisor")
        rule = self._create_rule_with_client(service_availability) if with_client else AgentRules(modules=[])
        errors = list(rule._validate_agent(resource))
        assert [error.code for error in errors] == expected_codes
