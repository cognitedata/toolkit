"""Detect whether the toolkit CLI is invoked by an AI coding agent.

Detection is environment-based and agent-agnostic. Rules follow the ordering in
Vercel's ``agents.json`` where possible, plus common signals for agents called
out in CDF-28539 (Windsurf, Cody, Amazon Q Developer, etc.).

See https://github.com/vercel/detect-agent for the ``AI_AGENT`` standard.
"""

import os
from collections.abc import Callable, Mapping
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel

from cognite_toolkit._cdf_tk.utils.cicd import get_cicd_environment

InvocationSource = Literal["cicd", "agent", "human"]
AI_AGENT_ENV_VAR = "AI_AGENT"


def _env_set(env: Mapping[str, str], name: str) -> bool:
    return bool(env.get(name))


def _any_env_set(env: Mapping[str, str], *names: str) -> bool:
    return any(_env_set(env, name) for name in names)


def _normalize_ai_agent_name(value: str) -> str:
    """Normalize ``AI_AGENT`` values such as ``claude-code@1.2``."""
    return value.split("@", maxsplit=1)[0].strip().lower()


def detect_coding_agent(env: Mapping[str, str] | None = None) -> str | None:
    """Return a normalized coding-agent id when env signals match, else ``None``.

    The first matching rule wins. Pass ``env`` in tests; production code uses
    ``os.environ``.
    """
    environment = os.environ if env is None else env

    if ai_agent := environment.get(AI_AGENT_ENV_VAR):
        if normalized := _normalize_ai_agent_name(ai_agent):
            return normalized

    checks: list[tuple[str, Callable[[Mapping[str, str]], bool]]] = [
        ("kimi", lambda e: _env_set(e, "KIMI_PLUGIN_ROOT")),
        ("grok", lambda e: _any_env_set(e, "GROK_PLUGIN_ROOT", "GROK_PLUGIN_DATA")),
        ("gemini-cli", lambda e: _env_set(e, "GEMINI_CLI")),
        ("cline", lambda e: _env_set(e, "CLINE_ACTIVE")),
        (
            "codex",
            lambda e: _any_env_set(e, "CODEX_SANDBOX", "CODEX_CI", "CODEX_THREAD_ID", "CODEX_SANDBOX_NETWORK_DISABLED"),
        ),
        ("antigravity", lambda e: _any_env_set(e, "ANTIGRAVITY_AGENT", "ANTIGRAVITY_CLI_ALIAS")),
        ("augment-cli", lambda e: _env_set(e, "AUGMENT_AGENT")),
        ("open-code", lambda e: _any_env_set(e, "OPENCODE_CLIENT", "OPENCODE")),
        ("goose", lambda e: _env_set(e, "GOOSE_PROVIDER")),
        ("junie", lambda e: _any_env_set(e, "JUNIE_DATA", "JUNIE_SHIM_PATH")),
        ("openclaw", lambda e: _env_set(e, "OPENCLAW_SHELL")),
        (
            "claude-cowork",
            lambda e: _env_set(e, "CLAUDE_CODE_IS_COWORK") and _any_env_set(e, "CLAUDECODE", "CLAUDE_CODE"),
        ),
        ("claude-code", lambda e: _any_env_set(e, "CLAUDECODE", "CLAUDE_CODE")),
        ("cursor-cli", lambda e: _env_set(e, "CURSOR_AGENT") or e.get("CURSOR_EXTENSION_HOST_ROLE") == "agent-exec"),
        ("cursor", lambda e: _env_set(e, "CURSOR_TRACE_ID")),
        (
            "github-copilot",
            lambda e: _any_env_set(
                e,
                "COPILOT_MODEL",
                "COPILOT_ALLOW_ALL",
                "COPILOT_GITHUB_TOKEN",
                "COPILOT_AGENT_SESSION_ID",
            ),
        ),
        ("windsurf", lambda e: _any_env_set(e, "WINDSURF_SESSION", "WINDSURF_TERMINAL", "CODEIUM_WINDSURF")),
        ("cody", lambda e: _any_env_set(e, "CODY_PARENT_PID", "SOURCEGRAPH_CODY_PRO_ACTIVE", "CODY_USER_AGENT")),
        ("amazon-q", lambda e: _any_env_set(e, "Q_CLI_VERSION", "AMAZON_Q_USER", "Q_DEVELOPER_CLI")),
        ("aider", lambda e: _any_env_set(e, "AIDER_EDITOR", "AIDER_YES")),
        ("replit", lambda e: _env_set(e, "REPL_ID")),
    ]

    for agent_id, predicate in checks:
        if predicate(environment):
            return agent_id

    return None


class InvocationInfo(BaseModel):
    """Who invoked the toolkit CLI."""

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    invocation_source: InvocationSource
    coding_agent: str | None = Field(default=None)

    def to_tracking_dict(self) -> dict[str, str]:
        payload: dict[str, str] = {"invocationSource": self.invocation_source}
        if self.coding_agent is not None:
            payload["codingAgent"] = self.coding_agent
        return payload


def get_invocation_info() -> InvocationInfo:
    """Classify the current invocation as CI/CD, coding agent, or human."""
    coding_agent = detect_coding_agent()
    cicd = get_cicd_environment()

    if cicd != "local":
        return InvocationInfo(invocation_source="cicd", coding_agent=coding_agent)
    if coding_agent is not None:
        return InvocationInfo(invocation_source="agent", coding_agent=coding_agent)
    return InvocationInfo(invocation_source="human")
