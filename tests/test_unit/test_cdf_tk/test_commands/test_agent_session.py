from pathlib import Path
from unittest.mock import MagicMock

from cognite_toolkit._cdf_tk.commands.agent_session import (
    AgentSession,
    clear_agent_session,
    load_agent_session,
    save_agent_session,
    session_file_path,
)


def test_session_roundtrip(tmp_path: Path) -> None:
    path = session_file_path(tmp_path)
    session = AgentSession(
        cursor="cursor-abc",
        agent_external_id="toolkit_cli_agent",
        project_dir=tmp_path.resolve().as_posix(),
        cdf_project="my-project",
        cdf_cluster="api.cognitedata.com",
    )
    save_agent_session(path, session)
    loaded = load_agent_session(path)
    assert loaded == session


def test_clear_session(tmp_path: Path) -> None:
    path = session_file_path(tmp_path)
    save_agent_session(
        path,
        AgentSession(
            cursor="cursor-abc",
            agent_external_id="toolkit_cli_agent",
            project_dir=tmp_path.resolve().as_posix(),
            cdf_project="my-project",
        ),
    )
    clear_agent_session(path)
    assert load_agent_session(path) is None


def test_session_matches_project_and_agent(tmp_path: Path) -> None:
    client = MagicMock()
    client.config.project = "my-project"
    client.config.cdf_cluster = "api.cognitedata.com"
    session = AgentSession(
        cursor="cursor-abc",
        agent_external_id="toolkit_cli_agent",
        project_dir=tmp_path.resolve().as_posix(),
        cdf_project="my-project",
        cdf_cluster="api.cognitedata.com",
    )
    assert session.matches(tmp_path, "toolkit_cli_agent", client)
    assert not session.matches(tmp_path, "other_agent", client)
