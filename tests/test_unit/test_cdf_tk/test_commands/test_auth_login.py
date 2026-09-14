from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.commands.auth.session_command import AuthSessionCommand, confirm_login_flow_overrides_env
from cognite_toolkit._cdf_tk.commands.auth.session_store import read_session, read_session_metadata
from cognite_toolkit._cdf_tk.constants import COGNITE_CLI_SESSION_VERSION
from cognite_toolkit._cdf_tk.exceptions import AuthenticationError

from tests.test_unit.test_cdf_tk.auth_test_helpers import browser_opener


def test_login_clears_unsupported_session_version_and_signs_in(
    sample_keyring: Path,
    cli_home: Path,
    ephemeral_port: int,
    cogidp_http,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (cli_home / "session.json").write_text(
        '{"version": 0, "org": "org", "accessTokenExpiresAt": "x", "refreshTokenExpiresAt": "y"}\n'
    )
    base_url, _ = cogidp_http("https://auth.example.com")
    monkeypatch.setenv("COGNITE_IDP_BASE_URL", base_url)
    monkeypatch.setattr(
        "cognite_toolkit._cdf_tk.commands.auth.oidc.webbrowser.open",
        browser_opener(ephemeral_port, succeeds=True),
    )

    AuthSessionCommand().login(org="my-org", force=False, port=ephemeral_port)

    assert read_session_metadata() is not None
    session = read_session()
    assert session is not None
    assert session.org == "my-org"
    assert session.version == COGNITE_CLI_SESSION_VERSION


def test_confirm_login_flow_proceeds_when_login_flow_not_in_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LOGIN_FLOW", raising=False)

    assert confirm_login_flow_overrides_env("session") is True


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
