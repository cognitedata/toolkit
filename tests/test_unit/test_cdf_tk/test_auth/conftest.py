import pytest


@pytest.fixture(autouse=True)
def shorten_session_login_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("cognite_toolkit._cdf_tk.commands.auth.oidc._LOGIN_TIMEOUT_SECONDS", 15)
