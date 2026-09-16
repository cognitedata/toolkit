from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.commands.auth.dotenv_path import describe_configured_login_flow, find_dotenv_path


def test_find_dotenv_path_prefers_cwd_over_parent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    parent_env = tmp_path.parent / ".env"
    cwd_env = tmp_path / ".env"
    parent_env.write_text("LOGIN_FLOW=client_credentials\n", encoding="utf-8")
    cwd_env.write_text("LOGIN_FLOW=session\n", encoding="utf-8")

    assert find_dotenv_path() == cwd_env


def test_describe_configured_login_flow_reports_dotenv_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    env_file = tmp_path.parent / ".env"
    env_file.write_text("LOGIN_FLOW=client_credentials\n", encoding="utf-8")
    monkeypatch.setenv("LOGIN_FLOW", "client_credentials")

    flow, source = describe_configured_login_flow()
    assert flow == "client_credentials"
    assert source is not None
    assert source.endswith(".env")
