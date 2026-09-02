"""Tests for AI coding agent invocation detection."""

from unittest.mock import patch

import pytest

from cognite_toolkit._cdf_tk.utils.coding_agent import detect_coding_agent, get_invocation_info


class TestDetectCodingAgent:
    def test_ai_agent_standard_takes_priority(self) -> None:
        env = {"AI_AGENT": "custom-agent@2.0", "CURSOR_TRACE_ID": "trace-1"}
        assert detect_coding_agent(env) == "custom-agent"

    @pytest.mark.parametrize(
        ("env", "expected"),
        [
            ({"CURSOR_AGENT": "1", "CURSOR_TRACE_ID": "trace-1"}, "cursor-cli"),
            ({"CURSOR_TRACE_ID": "trace-1"}, "cursor"),
            ({"CLAUDECODE": "1"}, "claude-code"),
            ({"COPILOT_AGENT_SESSION_ID": "session-1"}, "github-copilot"),
            ({"WINDSURF_SESSION": "session-1"}, "windsurf"),
            ({"CODY_PARENT_PID": "1234"}, "cody"),
            ({"Q_CLI_VERSION": "1.0.0"}, "amazon-q"),
            ({}, None),
        ],
        ids=[
            "cursor_cli_before_cursor",
            "cursor",
            "claude_code",
            "github_copilot",
            "windsurf",
            "cody",
            "amazon_q",
            "no_signals",
        ],
    )
    def test_detect_coding_agent(self, env: dict[str, str], expected: str | None) -> None:
        assert detect_coding_agent(env) == expected


class TestGetInvocationInfo:
    def test_human_local_shell(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            info = get_invocation_info()
        assert info.invocation_source == "human"
        assert info.coding_agent is None
        assert info.to_tracking_dict() == {"invocationSource": "human"}

    def test_agent_local_shell(self) -> None:
        with patch.dict("os.environ", {"CLAUDECODE": "1"}, clear=True):
            info = get_invocation_info()
        assert info.invocation_source == "agent"
        assert info.coding_agent == "claude-code"
        assert info.to_tracking_dict() == {"invocationSource": "agent", "codingAgent": "claude-code"}

    def test_cicd_without_agent(self) -> None:
        with patch.dict("os.environ", {"CI": "true", "GITHUB_ACTIONS": "true"}, clear=True):
            info = get_invocation_info()
        assert info.invocation_source == "cicd"
        assert info.coding_agent is None

    def test_cicd_with_copilot_agent(self) -> None:
        env = {"CI": "true", "GITHUB_ACTIONS": "true", "COPILOT_AGENT_SESSION_ID": "session-1"}
        with patch.dict("os.environ", env, clear=True):
            info = get_invocation_info()
        assert info.invocation_source == "cicd"
        assert info.coding_agent == "github-copilot"
