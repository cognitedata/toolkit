from pathlib import Path
from unittest.mock import patch

import pytest

from cognite_toolkit._cdf_tk.commands.auth import parse_login_flow_input
from cognite_toolkit._cdf_tk.commands.auth.session_command import AuthSessionCommand, confirm_login_flow_overrides_env
from cognite_toolkit._cdf_tk.commands.auth.session_store import StoredSession, read_session_metadata
from cognite_toolkit._cdf_tk.constants import COGNITE_CLI_SESSION_VERSION
from cognite_toolkit._cdf_tk.exceptions import AuthenticationError


def test_parse_login_flow_input_accepts_login_flow_and_cli_aliases() -> None:
    assert parse_login_flow_input("session") == "session"
    assert parse_login_flow_input("device_code") == "device_code"
    assert parse_login_flow_input("device-code") == "device_code"
    assert parse_login_flow_input("devicecode") == "device_code"
    assert parse_login_flow_input("client_credentials") == "client_credentials"
    assert parse_login_flow_input("client-credentials") == "client_credentials"
    assert parse_login_flow_input("clientcredentials") == "client_credentials"


def test_login_clears_corrupted_session_metadata(
    sample_keyring: Path, cli_home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (cli_home / "session.json").write_text(
        '{"version": 0, "org": "org", "accessTokenExpiresAt": "x", "refreshTokenExpiresAt": "y"}\n'
    )
    logged_in_org: list[str] = []

    def fake_login(org: str, port: int | None = None) -> StoredSession:
        assert read_session_metadata() is None
        logged_in_org.append(org)
        return StoredSession(
            version=COGNITE_CLI_SESSION_VERSION,
            org=org,
            access_token="access",
            refresh_token="refresh",
            access_token_expires_at="2026-01-01T01:00:00.000Z",
            refresh_token_expires_at="2026-01-02T01:00:00.000Z",
        )

    monkeypatch.setattr(
        "cognite_toolkit._cdf_tk.commands.auth.session_command.login_for_session",
        fake_login,
    )
    with patch("cognite_toolkit._cdf_tk.commands.auth.session_command.write_session"):
        AuthSessionCommand().login(org="my-org", force=False, port=None)

    assert logged_in_org == ["my-org"]


def test_confirm_login_flow_overrides_env_skips_without_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LOGIN_FLOW", raising=False)

    assert confirm_login_flow_overrides_env("session") is True


def test_confirm_login_flow_overrides_env_skips_matching_flow(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("LOGIN_FLOW", "device_code")

    assert confirm_login_flow_overrides_env("device_code") is True
    assert capsys.readouterr().out == ""


def test_confirm_login_flow_overrides_env_warns_on_mismatch(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("LOGIN_FLOW", "device_code")
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr(
        "cognite_toolkit._cdf_tk.commands.auth.session_command.questionary.confirm",
        lambda *args, **kwargs: type("Answer", (), {"unsafe_ask": lambda self: False})(),
    )

    assert confirm_login_flow_overrides_env("session") is False

    output = capsys.readouterr().out
    assert "LOGIN_FLOW='device_code'" in output
    assert "selected 'session'" in output


def test_confirm_login_flow_overrides_env_raises_without_tty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LOGIN_FLOW", "session")
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)

    with pytest.raises(AuthenticationError, match="LOGIN_FLOW='session'"):
        confirm_login_flow_overrides_env("device_code")
