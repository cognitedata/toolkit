import pytest

from cognite_toolkit._cdf_tk.commands.auth_session import confirm_login_flow_overrides_env
from cognite_toolkit._cdf_tk.exceptions import AuthenticationError
from cognite_toolkit._cdf_tk.utils.auth import parse_auth_login_flow


def test_parse_auth_login_flow_maps_cli_values() -> None:
    assert parse_auth_login_flow("session") == "session"
    assert parse_auth_login_flow("device-code") == "device_code"
    assert parse_auth_login_flow("client-credentials") == "client_credentials"


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
        "cognite_toolkit._cdf_tk.commands.auth_session.questionary.confirm",
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
