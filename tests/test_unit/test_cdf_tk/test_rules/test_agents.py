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

    def test_get_status_with_client(self, service_availability: ServicesAvailability) -> None:
        rule = self._create_rule_with_client(service_availability)
        status = rule.get_status()
        assert status.code == "ready"
        assert "validate agent models" in status.message.lower()

    def test_get_status_without_client(self) -> None:
        rule = AgentRules(modules=[])
        status = rule.get_status()
        assert status.code == "reduced"
        assert "requires a client" in status.message.lower()

    def test_get_status_when_endpoint_unavailable(self) -> None:
        mock_client = MagicMock()
        mock_client.tool.agents.service_availability.side_effect = Exception("boom")
        rule = AgentRules(modules=[], client=mock_client)
        status = rule.get_status()
        assert status.code == "reduced"
        assert "could not fetch" in status.message.lower()

    def test_validate_agent_unknown_model(self, tmp_path: Path, service_availability: ServicesAvailability) -> None:
        yaml_file = tmp_path / "agents" / "agent.yaml"
        self._write_agent_yaml(
            yaml_file,
            {"externalId": "my_agent", "name": "My Agent", "model": "gcp/claude-5-opus"},
        )
        resource = self._create_built_resource(yaml_file, yaml_file)
        rule = self._create_rule_with_client(service_availability)
        errors = list(rule._validate_agent(resource))
        assert len(errors) == 1
        assert isinstance(errors[0], ConsistencyError)
        assert errors[0].code == "AGENT-MODEL"
        assert "gcp/claude-5-opus" in errors[0].message

    def test_validate_agent_known_model(self, tmp_path: Path, service_availability: ServicesAvailability) -> None:
        yaml_file = tmp_path / "agents" / "agent.yaml"
        self._write_agent_yaml(
            yaml_file,
            {"externalId": "my_agent", "name": "My Agent", "model": "azure/gpt-4.1"},
        )
        resource = self._create_built_resource(yaml_file, yaml_file)
        rule = self._create_rule_with_client(service_availability)
        errors = list(rule._validate_agent(resource))
        assert len(errors) == 0

    def test_validate_agent_no_client_allows_any_model(self, tmp_path: Path) -> None:
        yaml_file = tmp_path / "agents" / "agent.yaml"
        self._write_agent_yaml(
            yaml_file,
            {"externalId": "my_agent", "name": "My Agent", "model": "some-brand-new-model"},
        )
        resource = self._create_built_resource(yaml_file, yaml_file)
        rule = AgentRules(modules=[])
        errors = list(rule._validate_agent(resource))
        assert len(errors) == 0

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

    def test_validate_agent_subagents_unsupported_runtime_version(
        self, tmp_path: Path, service_availability: ServicesAvailability
    ) -> None:
        yaml_file = tmp_path / "agents" / "agent.yaml"
        self._write_agent_yaml(
            yaml_file,
            {
                "externalId": "supervisor",
                "name": "Supervisor",
                "model": "azure/gpt-4.1",
                "runtimeVersion": "1.0.0",
                "subagents": [{"agentExternalId": "specialist"}],
            },
        )
        resource = self._create_built_resource(yaml_file, yaml_file, external_id="supervisor")
        rule = self._create_rule_with_client(service_availability)
        errors = list(rule._validate_agent(resource))
        assert len(errors) == 1
        assert errors[0].code == "AGENT-SUBAGENTS-RUNTIME-VERSION"

    def test_validate_agent_subagents_supported_runtime_version(
        self, tmp_path: Path, service_availability: ServicesAvailability
    ) -> None:
        yaml_file = tmp_path / "agents" / "agent.yaml"
        self._write_agent_yaml(
            yaml_file,
            {
                "externalId": "supervisor",
                "name": "Supervisor",
                "model": "azure/gpt-4.1",
                "runtimeVersion": "1.3.0",
                "subagents": [{"agentExternalId": "specialist"}],
            },
        )
        resource = self._create_built_resource(yaml_file, yaml_file, external_id="supervisor")
        rule = self._create_rule_with_client(service_availability)
        errors = list(rule._validate_agent(resource))
        assert len(errors) == 0

    def test_validate_agent_subagents_unknown_runtime_version_is_allowed(
        self, tmp_path: Path, service_availability: ServicesAvailability
    ) -> None:
        """A runtime version not present in the availability response should not be blocked."""
        yaml_file = tmp_path / "agents" / "agent.yaml"
        self._write_agent_yaml(
            yaml_file,
            {
                "externalId": "supervisor",
                "name": "Supervisor",
                "model": "azure/gpt-4.1",
                "runtimeVersion": "9.9.9",
                "subagents": [{"agentExternalId": "specialist"}],
            },
        )
        resource = self._create_built_resource(yaml_file, yaml_file, external_id="supervisor")
        rule = self._create_rule_with_client(service_availability)
        errors = list(rule._validate_agent(resource))
        assert len(errors) == 0

    def test_service_availability_returns_none_without_client(self) -> None:
        rule = AgentRules(modules=[])
        assert rule.service_availability is None

    def test_service_availability_returns_none_on_error(self) -> None:
        mock_client = MagicMock()
        mock_client.tool.agents.service_availability.side_effect = Exception("boom")
        rule = AgentRules(modules=[], client=mock_client)
        assert rule.service_availability is None
