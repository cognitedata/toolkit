from pathlib import Path

from pydantic import BaseModel

from cognite_toolkit._cdf_tk.client import ToolkitClient


class AgentSession(BaseModel):
    cursor: str
    agent_external_id: str
    project_dir: str
    cdf_project: str
    cdf_cluster: str = ""

    def matches(self, project_root: Path, agent_external_id: str, client: ToolkitClient) -> bool:
        config = client.config
        return (
            self.project_dir == project_root.resolve().as_posix()
            and self.agent_external_id == agent_external_id
            and self.cdf_project == config.project
            and self.cdf_cluster == (config.cdf_cluster or "")
        )


def session_file_path(project_root: Path) -> Path:
    return project_root / ".cdf" / "agent-session.json"


def load_agent_session(path: Path) -> AgentSession | None:
    if not path.is_file():
        return None
    try:
        return AgentSession.model_validate_json(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def save_agent_session(path: Path, session: AgentSession) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(session.model_dump_json(indent=2) + "\n", encoding="utf-8")


def clear_agent_session(path: Path) -> None:
    if path.is_file():
        path.unlink()


def is_stale_session_error(message: str) -> bool:
    lowered = message.lower()
    return any(
        token in lowered for token in ("cursor", "session", "expired", "invalid", "not found", "unknown conversation")
    )
