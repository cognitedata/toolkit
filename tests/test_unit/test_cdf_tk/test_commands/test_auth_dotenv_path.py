from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.commands.auth.dotenv_path import describe_configured_login_flow, find_dotenv_path


def test_find_dotenv_path_prefers_cwd_over_parent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    subdir = tmp_path / "subdir"
    subdir.mkdir()
    monkeypatch.chdir(subdir)
    parent_env = tmp_path / ".env"
    cwd_env = subdir / ".env"
    parent_env.write_text("LOGIN_FLOW=client_credentials\n", encoding="utf-8")
    cwd_env.write_text("LOGIN_FLOW=session\n", encoding="utf-8")

    assert find_dotenv_path() == cwd_env.resolve()


def test_find_dotenv_path_relative_cwd_checks_parent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    subdir = tmp_path / "subdir"
    subdir.mkdir()
    parent_env = tmp_path / ".env"
    parent_env.write_text("LOGIN_FLOW=client_credentials\n", encoding="utf-8")
    monkeypatch.chdir(subdir)

    assert find_dotenv_path(Path(".")) == parent_env.resolve()


def test_describe_configured_login_flow_reports_dotenv_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    subdir = tmp_path / "subdir"
    subdir.mkdir()
    monkeypatch.chdir(subdir)
    env_file = tmp_path / ".env"
    env_file.write_text("LOGIN_FLOW=client_credentials\n", encoding="utf-8")
    monkeypatch.setenv("LOGIN_FLOW", "client_credentials")

    flow, source = describe_configured_login_flow()
    assert flow == "client_credentials"
    assert source is not None
    assert Path(source).resolve() == env_file.resolve()


def test_describe_configured_login_flow_unreadable_dotenv_uses_env_var(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("LOGIN_FLOW=client_credentials\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("LOGIN_FLOW", "client_credentials")

    def _unreadable(_path: Path) -> dict[str, str | None]:
        raise OSError("unreadable")

    monkeypatch.setattr("cognite_toolkit._cdf_tk.commands.auth.dotenv_path.dotenv_values", _unreadable)

    flow, source = describe_configured_login_flow()
    assert flow == "client_credentials"
    assert source == "the LOGIN_FLOW environment variable"
