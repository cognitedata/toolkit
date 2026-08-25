from pathlib import Path
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.client.resource_classes.agent_chat import (
    AgentChatResponse,
    ClientToolCall,
    ClientToolCallSpec,
)
from cognite_toolkit._cdf_tk.commands.agent import (
    DEFAULT_AGENT_MODEL,
    AgentCommand,
    _resolve_path,
    _tool_list_dir,
    _tool_read_file,
    build_instructions,
    ensure_agent,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.plugins import Plugins


def test_agent_plugin_is_registered() -> None:
    assert "agent" in {plugin.value.name for plugin in Plugins}


def test_build_instructions_includes_project_root(tmp_path: Path) -> None:
    instructions = build_instructions(tmp_path, "dev")
    assert tmp_path.resolve().as_posix() in instructions
    assert "dev" in instructions


class TestAgentCommandValidation:
    def test_empty_prompt_raises(self, tmp_path: Path) -> None:
        cmd = AgentCommand(skip_tracking=True)
        with pytest.raises(ToolkitValueError, match="No prompt provided"):
            cmd.execute("   ", tmp_path, "dev", None, None, "acceptEdits", 50, False, False)

    def test_invalid_permission_mode_raises(self, tmp_path: Path) -> None:
        cmd = AgentCommand(skip_tracking=True)
        with pytest.raises(ToolkitValueError, match="Invalid permission mode"):
            cmd.execute("do something", tmp_path, "dev", None, None, "yolo", 50, False, False)

    def test_missing_project_dir_raises(self, tmp_path: Path) -> None:
        cmd = AgentCommand(skip_tracking=True)
        missing = tmp_path / "does-not-exist"
        with pytest.raises(ToolkitValueError, match="does not exist"):
            cmd.execute("do something", missing, "dev", None, None, "acceptEdits", 50, False, False)


class TestLocalTools:
    def test_resolve_path_rejects_escape(self, tmp_path: Path) -> None:
        with pytest.raises(ToolkitValueError, match="escapes"):
            _resolve_path(tmp_path, "../outside")

    def test_list_and_read_file(self, tmp_path: Path) -> None:
        (tmp_path / "modules").mkdir()
        config = tmp_path / "cdf.toml"
        config.write_text("x = 1\n", encoding="utf-8")
        listing = _tool_list_dir(tmp_path, ".")
        assert "cdf.toml" in listing
        content = _tool_read_file(tmp_path, "cdf.toml", None, None)
        assert content == "x = 1"


class TestAgentCommandLoop:
    def test_single_tool_round_trip(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        (tmp_path / "cdf.toml").write_text("plugins = {}\n", encoding="utf-8")
        first = AgentChatResponse.model_validate(
            {
                "agentExternalId": "cdf_toolkit_cli",
                "response": {
                    "type": "result",
                    "cursor": "c1",
                    "messages": [
                        {
                            "role": "agent",
                            "content": {"type": "text", "text": "Reading config."},
                            "actions": [
                                {
                                    "type": "clientTool",
                                    "actionId": "a1",
                                    "clientTool": {
                                        "name": "read_file",
                                        "arguments": '{"path": "cdf.toml"}',
                                    },
                                }
                            ],
                        }
                    ],
                },
            }
        )
        second = AgentChatResponse.model_validate(
            {
                "agentExternalId": "cdf_toolkit_cli",
                "response": {
                    "type": "result",
                    "cursor": None,
                    "messages": [
                        {
                            "role": "agent",
                            "content": {"type": "text", "text": "Your project has cdf.toml."},
                        }
                    ],
                },
            }
        )

        mock_client = MagicMock()
        mock_client.tool.agents.chat.side_effect = [first, second]
        mock_client.tool.agents.retrieve.return_value = [MagicMock()]
        mock_client.config.project = "test-project"
        mock_client.config.cdf_cluster = "api.cognitedata.com"

        env_vars = MagicMock()
        env_vars.get_client.return_value = mock_client

        monkeypatch.setattr(
            "cognite_toolkit._cdf_tk.commands.agent.EnvironmentVariables.create_from_environment",
            lambda: env_vars,
        )
        monkeypatch.setattr(
            "cognite_toolkit._cdf_tk.commands.agent.questionary.confirm",
            lambda *args, **kwargs: MagicMock(unsafe_ask=lambda: True),
        )

        cmd = AgentCommand(skip_tracking=True)
        cmd.execute(
            "what config do I have?", tmp_path, "dev", "toolkit_cli_agent", None, "acceptEdits", 50, True, False
        )

        assert mock_client.tool.agents.chat.call_count == 2
        second_call = mock_client.tool.agents.chat.call_args_list[1]
        assert second_call.args[0] == "toolkit_cli_agent"
        assert second_call.kwargs["cursor"] == "c1"

    def test_reserved_agent_uses_existing(self) -> None:
        client = MagicMock()
        client.tool.agents.retrieve.return_value = [MagicMock()]
        external_id = ensure_agent(client, "cdf_toolkit_cli", "instructions", None)
        assert external_id == "cdf_toolkit_cli"
        client.tool.agents.create.assert_not_called()

    def test_reserved_agent_missing_raises(self) -> None:
        client = MagicMock()
        client.tool.agents.retrieve.return_value = []
        with pytest.raises(ToolkitValueError, match="not available"):
            ensure_agent(client, "cdf_toolkit_cli", "instructions", None)

    def test_ensure_agent_creates_when_missing(self) -> None:
        client = MagicMock()
        client.tool.agents.retrieve.return_value = []
        external_id = ensure_agent(client, "my_toolkit_agent", "instructions", None)
        assert external_id == "my_toolkit_agent"
        client.tool.agents.create.assert_called_once()
        created = client.tool.agents.create.call_args[0][0][0]
        assert created.model == DEFAULT_AGENT_MODEL

    def test_summarize_tool_call(self) -> None:
        call = ClientToolCall(
            action_id="a1",
            client_tool=ClientToolCallSpec(name="read_file", arguments={"path": "cdf.toml"}),
        )
        summary = AgentCommand._summarize_tool_call(call)
        assert "read_file" in summary
        assert "cdf.toml" in summary
