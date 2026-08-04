import fnmatch
import re
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any

import questionary
from rich.console import Console
from rich.markup import escape
from rich.panel import Panel

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.agent import AgentRequest
from cognite_toolkit._cdf_tk.client.resource_classes.agent_chat import (
    AgentChatResponse,
    ChatActionResult,
    ChatMessage,
    ClientToolAction,
    ClientToolActionDefinition,
    ClientToolCall,
    ToolConfirmationCall,
    ToolConfirmationResult,
)
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.commands.agent_session import (
    AgentSession,
    clear_agent_session,
    is_stale_session_error,
    load_agent_session,
    save_agent_session,
    session_file_path,
)
from cognite_toolkit._cdf_tk.exceptions import AuthorizationError, ToolkitValueError
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables

DEFAULT_AGENT_EXTERNAL_ID = "toolkit_cli_agent"
DEFAULT_AGENT_MODEL = "aws/claude-4.5-haiku"
# CDF-reserved IDs cannot be created via the API; they may exist only when enabled in a project.
RESERVED_AGENT_EXTERNAL_IDS = frozenset({"cdf_toolkit_cli"})
PERMISSION_MODES = ("default", "acceptEdits", "readOnly")

_AGENT_INSTRUCTIONS = """\
You are an expert Cognite Data Fusion (CDF) Toolkit engineer working directly inside a
user's Toolkit project from the command line. The user describes what they want in
natural language and you make it happen using the files in this project and the `cdf` CLI.

## Project layout

A CDF Toolkit project is a directory tree of declarative YAML (plus SQL, Python and
GraphQL) describing CDF resources. The `cdf.toml` file at the project root is the project
marker. Toolkit modules and their resource directories live under `modules/<module_name>/`.

## How to work

1. Understand first — use list_dir, read_file, grep and glob to inspect the project before
   changing anything. Match existing naming conventions, directory layout and formatting.
2. Make focused changes — create or edit files under `modules/<module_name>/`. Never create
   resource directories at the project root.
3. Validate — run `cdf build --verbose` via run_cdf after changes. If the build fails, read
   the error, fix the configuration and rebuild.
4. Be transparent — briefly explain what you changed and why.

## Safety

- Read-only operations need no confirmation.
- Before destructive CDF operations (`cdf deploy` without --dry-run, `cdf clean`), clearly
  state what will happen. The client will ask the user to confirm.
- Prefer `cdf deploy --dry-run` to preview changes.
"""

CLIENT_TOOLS: list[ClientToolAction] = [
    ClientToolAction(
        client_tool=ClientToolActionDefinition(
            name="list_dir",
            description="List files and directories under a path relative to the project root.",
            parameters={
                "type": "object",
                "properties": {"path": {"type": "string", "description": "Relative directory path."}},
                "required": ["path"],
            },
        )
    ),
    ClientToolAction(
        client_tool=ClientToolActionDefinition(
            name="read_file",
            description="Read a text file relative to the project root.",
            parameters={
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Relative file path."},
                    "start_line": {"type": "integer", "description": "Optional 1-based start line."},
                    "end_line": {"type": "integer", "description": "Optional 1-based end line (inclusive)."},
                },
                "required": ["path"],
            },
        )
    ),
    ClientToolAction(
        client_tool=ClientToolActionDefinition(
            name="grep",
            description="Search for a regex pattern in project files.",
            parameters={
                "type": "object",
                "properties": {
                    "pattern": {"type": "string", "description": "Regex pattern to search for."},
                    "path": {"type": "string", "description": "Relative directory or file to search."},
                    "glob": {"type": "string", "description": "Optional filename glob filter."},
                },
                "required": ["pattern"],
            },
        )
    ),
    ClientToolAction(
        client_tool=ClientToolActionDefinition(
            name="glob",
            description="Find files matching a glob pattern under the project root.",
            parameters={
                "type": "object",
                "properties": {
                    "pattern": {"type": "string", "description": "Glob pattern, e.g. modules/**/*.yaml"},
                },
                "required": ["pattern"],
            },
        )
    ),
    ClientToolAction(
        client_tool=ClientToolActionDefinition(
            name="write_file",
            description="Create or overwrite a text file relative to the project root.",
            parameters={
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Relative file path."},
                    "content": {"type": "string", "description": "Full file content."},
                },
                "required": ["path", "content"],
            },
        )
    ),
    ClientToolAction(
        client_tool=ClientToolActionDefinition(
            name="edit_file",
            description="Replace an exact string in a file (all occurrences).",
            parameters={
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Relative file path."},
                    "old_string": {"type": "string", "description": "Exact text to replace."},
                    "new_string": {"type": "string", "description": "Replacement text."},
                },
                "required": ["path", "old_string", "new_string"],
            },
        )
    ),
    ClientToolAction(
        client_tool=ClientToolActionDefinition(
            name="run_cdf",
            description=(
                "Run a whitelisted cdf subcommand in the project directory. "
                "Allowed: build, deploy (--dry-run), modules list, auth verify, clean (with confirmation)."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "command": {
                        "type": "string",
                        "description": "cdf arguments without the 'cdf' prefix, e.g. 'build --verbose'.",
                    },
                },
                "required": ["command"],
            },
        )
    ),
]

_MUTATING_TOOLS = frozenset({"write_file", "edit_file", "run_cdf"})


def build_instructions(project_root: Path, env_name: str | None) -> str:
    lines = [
        _AGENT_INSTRUCTIONS,
        "## Session context",
        f"- Toolkit project root: {project_root.resolve().as_posix()}",
    ]
    if env_name:
        lines.append(f"- Default build environment: {env_name}")
    return "\n".join(lines) + "\n"


def ensure_agent(
    client: ToolkitClient,
    external_id: str,
    instructions: str,
    model: str | None,
) -> str:
    if external_id in RESERVED_AGENT_EXTERNAL_IDS:
        existing = client.tool.agents.retrieve([ExternalId(external_id=external_id)], ignore_unknown_ids=True)
        if existing:
            return external_id
        raise ToolkitValueError(
            f"Built-in agent {external_id!r} is not available in this CDF project. "
            "Omit --agent to use the auto-provisioned toolkit_cli_agent, or pass another agent external ID."
        )
    existing = client.tool.agents.retrieve([ExternalId(external_id=external_id)], ignore_unknown_ids=True)
    if existing:
        return external_id
    resolved_model = model if model is not None else DEFAULT_AGENT_MODEL
    request = AgentRequest(
        external_id=external_id,
        name="CDF Toolkit CLI agent",
        instructions=instructions,
        model=resolved_model,
        tools=[],
    )
    try:
        client.tool.agents.create([request])
    except Exception as exc:
        raise AuthorizationError(
            "Could not create the default agent in CDF. Ensure you have agents:write capability, "
            f"or pass --agent with an existing agent external ID. ({exc})"
        ) from exc
    return external_id


class AgentCommand(ToolkitCommand):
    def execute(
        self,
        prompt: str,
        organization_dir: Path,
        env_name: str | None,
        agent_external_id: str | None,
        model: str | None,
        permission_mode: str,
        max_steps: int,
        new_session: bool,
        verbose: bool,
    ) -> None:
        prompt = prompt.strip()
        if not prompt:
            raise ToolkitValueError("No prompt provided. Usage: cdf . <your request>")
        if permission_mode not in PERMISSION_MODES:
            raise ToolkitValueError(
                f"Invalid permission mode {permission_mode!r}. Choose one of: {', '.join(PERMISSION_MODES)}."
            )
        if not organization_dir.is_dir():
            raise ToolkitValueError(f"The project directory {organization_dir.as_posix()!r} does not exist.")

        env_vars = EnvironmentVariables.create_from_environment()
        client = env_vars.get_client()
        project_root = organization_dir.resolve()
        agent_id = agent_external_id or DEFAULT_AGENT_EXTERNAL_ID
        instructions = build_instructions(project_root, env_name)
        agent_id = ensure_agent(client, agent_id, instructions, model)

        console = Console()
        session_path = session_file_path(project_root)
        if new_session:
            clear_agent_session(session_path)

        cursor: str | None = None
        if not new_session:
            stored_session = load_agent_session(session_path)
            if stored_session:
                if stored_session.matches(project_root, agent_id, client):
                    cursor = stored_session.cursor
                    console.print("[dim]Continuing previous conversation.[/dim]")
                else:
                    clear_agent_session(session_path)
                    console.print("[dim]Starting a new conversation (project, agent or CDF target changed).[/dim]")

        console.print(Panel(escape(prompt), title="Prompt", style="cyan", expand=False))

        response = self._chat(client, agent_id, ChatMessage.from_text(prompt), cursor, session_path, console)
        self._save_session(client, agent_id, project_root, session_path, response)
        self._print_response_text(console, response)

        for step in range(max_steps):
            pending = self._collect_pending_actions(response)
            if not pending:
                return
            results: list[ChatActionResult | ToolConfirmationResult] = []
            for action in pending:
                if isinstance(action, ToolConfirmationCall):
                    if not self._confirm_tool_confirmation(console, action, permission_mode):
                        results.append(ToolConfirmationResult(action_id=action.action_id, status="DENY"))
                        continue
                    results.append(ToolConfirmationResult(action_id=action.action_id, status="ALLOW"))
                    continue
                if isinstance(action, ClientToolCall):
                    if not self._confirm_client_tool(console, action, permission_mode):
                        results.append(ChatActionResult.from_text(action.action_id, "User denied this action."))
                        continue
                    console.print(f"[dim]→ {action.name}[/dim]")
                    try:
                        output = self._dispatch_tool(action, project_root, env_name)
                    except Exception as exc:
                        output = f"Tool error: {exc}"
                        if verbose:
                            console.print(f"[red]{escape(output)}[/red]")
                    else:
                        if verbose:
                            preview = output if len(output) <= 500 else output[:497] + "..."
                            console.print(f"[dim]{escape(preview)}[/dim]")
                    results.append(ChatActionResult.from_text(action.action_id, output))

            response = self._chat(client, agent_id, results, response.cursor, session_path, console)
            self._save_session(client, agent_id, project_root, session_path, response)
            self._print_response_text(console, response)
        else:
            console.print("[yellow]Stopped: reached --max-steps without finishing.[/yellow]")

    @staticmethod
    def _chat(
        client: ToolkitClient,
        agent_id: str,
        messages: ChatMessage
        | ChatActionResult
        | ToolConfirmationResult
        | list[ChatActionResult | ToolConfirmationResult],
        cursor: str | None,
        session_path: Path,
        console: Console,
    ) -> AgentChatResponse:
        try:
            return client.tool.agents.chat(agent_id, messages, cursor=cursor, actions=CLIENT_TOOLS)
        except ToolkitAPIError as exc:
            if cursor is None or not is_stale_session_error(str(exc)):
                raise
            clear_agent_session(session_path)
            console.print("[yellow]Previous session expired; starting a new conversation.[/yellow]")
            return client.tool.agents.chat(agent_id, messages, actions=CLIENT_TOOLS)

    @staticmethod
    def _save_session(
        client: ToolkitClient,
        agent_id: str,
        project_root: Path,
        session_path: Path,
        response: AgentChatResponse,
    ) -> None:
        if not response.cursor:
            return
        config = client.config
        save_agent_session(
            session_path,
            AgentSession(
                cursor=response.cursor,
                agent_external_id=agent_id,
                project_dir=project_root.resolve().as_posix(),
                cdf_project=config.project,
                cdf_cluster=config.cdf_cluster or "",
            ),
        )

    @staticmethod
    def _collect_pending_actions(response: AgentChatResponse) -> list[ClientToolCall | ToolConfirmationCall]:
        return [*response.action_calls, *response.tool_confirmation_calls]

    @staticmethod
    def _print_response_text(console: Console, response: AgentChatResponse) -> None:
        if text := response.text:
            console.print(text, markup=False, highlight=False)

    @staticmethod
    def _confirm_client_tool(console: Console, call: ClientToolCall, permission_mode: str) -> bool:
        if permission_mode == "readOnly" and call.name in _MUTATING_TOOLS:
            console.print(f"[yellow]Skipped {call.name}: read-only mode.[/yellow]")
            return False
        if permission_mode == "acceptEdits" and call.name in {"write_file", "edit_file"}:
            return True
        if call.name not in _MUTATING_TOOLS:
            return True
        if call.name == "run_cdf":
            args = _parse_cdf_args(call.arguments.get("command", ""))
            if not _is_destructive_cdf(args):
                return True
        summary = AgentCommand._summarize_tool_call(call)
        return bool(questionary.confirm(f"Allow {summary}?", default=False).unsafe_ask())

    @staticmethod
    def _confirm_tool_confirmation(
        console: Console,
        call: ToolConfirmationCall,
        permission_mode: str,
    ) -> bool:
        if permission_mode == "readOnly":
            console.print(f"[yellow]Skipped server tool {call.tool_name}: read-only mode.[/yellow]")
            return False
        message = call.tool_description or f"Run server tool {call.tool_name}?"
        return bool(questionary.confirm(message, default=False).unsafe_ask())

    @staticmethod
    def _summarize_tool_call(call: ClientToolCall) -> str:
        args = call.arguments
        for key in ("path", "pattern", "command"):
            if isinstance(args.get(key), str):
                value = args[key]
                short = value if len(value) <= 80 else value[:77] + "..."
                return f"{call.name} ({short})"
        return call.name

    @staticmethod
    def _dispatch_tool(
        call: ClientToolCall,
        project_root: Path,
        env_name: str | None,
    ) -> str:
        args = call.arguments
        name = call.name
        if name == "list_dir":
            return _tool_list_dir(project_root, str(args.get("path", ".")))
        if name == "read_file":
            return _tool_read_file(
                project_root,
                str(args["path"]),
                args.get("start_line"),
                args.get("end_line"),
            )
        if name == "grep":
            return _tool_grep(
                project_root,
                str(args["pattern"]),
                str(args.get("path", ".")),
                str(args.get("glob", "*")),
            )
        if name == "glob":
            return _tool_glob(project_root, str(args["pattern"]))
        if name == "write_file":
            return _tool_write_file(project_root, str(args["path"]), str(args["content"]))
        if name == "edit_file":
            return _tool_edit_file(
                project_root,
                str(args["path"]),
                str(args["old_string"]),
                str(args["new_string"]),
            )
        if name == "run_cdf":
            return _tool_run_cdf(project_root, str(args["command"]), env_name)
        raise ToolkitValueError(f"Unknown tool {name!r}")


def _resolve_path(project_root: Path, relative_path: str) -> Path:
    target = (project_root / relative_path).resolve()
    if not target.is_relative_to(project_root):
        raise ToolkitValueError(f"Path {relative_path!r} escapes the project directory.")
    return target


def _tool_list_dir(project_root: Path, relative_path: str) -> str:
    target = _resolve_path(project_root, relative_path)
    if not target.is_dir():
        raise ToolkitValueError(f"Not a directory: {relative_path!r}")
    entries = sorted(target.iterdir(), key=lambda p: (not p.is_dir(), p.name.lower()))
    lines = []
    for entry in entries[:200]:
        suffix = "/" if entry.is_dir() else ""
        rel = entry.relative_to(project_root).as_posix()
        lines.append(f"{rel}{suffix}")
    if len(entries) > 200:
        lines.append(f"... and {len(entries) - 200} more")
    return "\n".join(lines) or "(empty directory)"


def _tool_read_file(
    project_root: Path,
    relative_path: str,
    start_line: Any,
    end_line: Any,
) -> str:
    target = _resolve_path(project_root, relative_path)
    if not target.is_file():
        raise ToolkitValueError(f"Not a file: {relative_path!r}")
    lines = target.read_text(encoding="utf-8").splitlines()
    start = int(start_line) if start_line else 1
    end = int(end_line) if end_line else len(lines)
    selected = lines[max(start - 1, 0) : end]
    return "\n".join(selected)


def _tool_grep(project_root: Path, pattern: str, relative_path: str, glob_pattern: str) -> str:
    target = _resolve_path(project_root, relative_path)
    regex = re.compile(pattern)
    matches: list[str] = []
    paths = [target] if target.is_file() else (p for p in target.rglob("*") if p.is_file())
    for path in paths:
        if target.is_dir() and not fnmatch.fnmatch(path.name, glob_pattern):
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        for line_no, line in enumerate(text.splitlines(), start=1):
            if regex.search(line):
                rel = path.relative_to(project_root).as_posix()
                matches.append(f"{rel}:{line_no}:{line}")
                if len(matches) >= 100:
                    return "\n".join(matches) + "\n... (truncated)"
    return "\n".join(matches) or "No matches."


def _tool_glob(project_root: Path, pattern: str) -> str:
    matches = sorted(project_root.glob(pattern))[:200]
    lines = [path.relative_to(project_root).as_posix() for path in matches if path.is_file()]
    if len(matches) > 200:
        lines.append("... (truncated)")
    return "\n".join(lines) or "No files matched."


def _tool_write_file(project_root: Path, relative_path: str, content: str) -> str:
    target = _resolve_path(project_root, relative_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")
    return f"Wrote {relative_path} ({len(content)} bytes)."


def _tool_edit_file(project_root: Path, relative_path: str, old_string: str, new_string: str) -> str:
    target = _resolve_path(project_root, relative_path)
    if not target.is_file():
        raise ToolkitValueError(f"Not a file: {relative_path!r}")
    text = target.read_text(encoding="utf-8")
    if old_string not in text:
        raise ToolkitValueError(f"old_string not found in {relative_path!r}")
    count = text.count(old_string)
    target.write_text(text.replace(old_string, new_string), encoding="utf-8")
    return f"Replaced {count} occurrence(s) in {relative_path}."


def _parse_cdf_args(command: str) -> list[str]:
    return shlex.split(command)


def _is_allowed_cdf(args: list[str]) -> bool:
    if not args:
        return False
    subcommand = args[0]
    if subcommand in {"build", "auth"}:
        return True
    if subcommand == "modules" and len(args) > 1 and args[1] == "list":
        return True
    if subcommand == "deploy":
        return True
    if subcommand == "clean":
        return True
    return False


def _is_destructive_cdf(args: list[str]) -> bool:
    if not args:
        return True
    subcommand = args[0]
    if subcommand == "deploy" and "--dry-run" not in args and "-r" not in args:
        return True
    if subcommand == "clean":
        return True
    return False


def _tool_run_cdf(project_root: Path, command: str, env_name: str | None) -> str:
    args = _parse_cdf_args(command)
    if not _is_allowed_cdf(args):
        raise ToolkitValueError(
            f"Command not allowed: {command!r}. Allowed: build, deploy, modules list, auth verify, clean."
        )
    if env_name and "-e" not in args and "--env" not in args and args and args[0] == "build":
        args.extend(["--env", env_name])
    completed = subprocess.run(
        [sys.executable, "-m", "cognite_toolkit._cdf", *args],
        cwd=project_root,
        capture_output=True,
        text=True,
        timeout=600,
    )
    output = (completed.stdout or "") + (completed.stderr or "")
    if completed.returncode != 0:
        return f"Exit code {completed.returncode}\n{output}".strip()
    return output.strip() or "(command completed with no output)"
