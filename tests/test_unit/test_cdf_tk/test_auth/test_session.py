from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest
import respx

from cognite_toolkit._cdf_tk.commands.auth.oidc import (
    _CallbackContext,
    _OAuthCallbackServer,
    build_session_from_tokens,
    login_for_session,
    refresh_session_tokens,
)
from cognite_toolkit._cdf_tk.commands.auth.session_keyring import delete_session_token
from cognite_toolkit._cdf_tk.commands.auth.session_refresh import ensure_fresh_session
from cognite_toolkit._cdf_tk.commands.auth.session_store import (
    SessionMetadata,
    StoredSession,
    read_session,
    read_session_metadata,
    token_state,
    write_session,
)
from cognite_toolkit._cdf_tk.constants import COGNITE_CLI_SESSION_VERSION
from cognite_toolkit._cdf_tk.exceptions import AuthenticationError

from tests.test_unit.test_cdf_tk.auth_test_helpers import browser_opener


def test_login_for_session_returns_tokens_after_oauth_callback(
    monkeypatch: pytest.MonkeyPatch,
    ephemeral_port: int,
    cogidp_http,
) -> None:
    base_url, _ = cogidp_http("https://auth.example.com")
    monkeypatch.setenv("COGNITE_IDP_BASE_URL", base_url)
    monkeypatch.setattr(
        "cognite_toolkit._cdf_tk.commands.auth.oidc.webbrowser.open",
        browser_opener(ephemeral_port, succeeds=True),
    )

    session = login_for_session("my-org", port=ephemeral_port)

    assert session.org == "my-org"
    assert session.access_token == "access-token"
    assert session.refresh_token == "refresh-token"


def test_session_login_prints_authorize_url_when_browser_unavailable(
    monkeypatch: pytest.MonkeyPatch,
    sample_keyring: Path,
    ephemeral_port: int,
    cogidp_http,
    capsys: pytest.CaptureFixture[str],
) -> None:
    base_url, _ = cogidp_http("https://auth.example.com")
    monkeypatch.setenv("COGNITE_IDP_BASE_URL", base_url)
    monkeypatch.setattr(
        "cognite_toolkit._cdf_tk.commands.auth.oidc.webbrowser.open",
        browser_opener(ephemeral_port, succeeds=False),
    )

    login_for_session("my-org", port=ephemeral_port)

    captured = capsys.readouterr()
    assert "Could not open browser automatically" in captured.out
    assert f"{base_url}/authorize?" in captured.out


def test_build_session_from_tokens_requires_refresh_token() -> None:
    with pytest.raises(AuthenticationError, match="refresh token"):
        build_session_from_tokens("my-org", {"access_token": "abc"})


def test_build_session_from_tokens_sets_expiry() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    session = build_session_from_tokens(
        "my-org",
        {"access_token": "access", "refresh_token": "refresh", "expires_in": 3600},
        now=now,
    )
    assert session.org == "my-org"
    assert session.version == COGNITE_CLI_SESSION_VERSION
    assert session.access_token_expires_at == "2026-01-01T01:00:00.000Z"


def test_token_state_expiring_within_leeway() -> None:
    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    metadata = SessionMetadata(
        version=1,
        org="org",
        access_token_expires_at=(now + timedelta(minutes=3)).isoformat(),
        refresh_token_expires_at=(now + timedelta(hours=10)).isoformat(),
    )
    assert token_state(metadata, now=now) == "EXPIRING"


def test_token_state_expired_when_refresh_past() -> None:
    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    metadata = SessionMetadata(
        version=1,
        org="org",
        access_token_expires_at=(now - timedelta(hours=1)).isoformat(),
        refresh_token_expires_at=(now - timedelta(minutes=1)).isoformat(),
    )
    assert token_state(metadata, now=now) == "EXPIRED"


def test_oauth_callback_page_shows_idp_error_message(ephemeral_port: int) -> None:
    context = _CallbackContext(
        expected_state="state",
        code_verifier="verifier",
        client_id="test-client",
        token_endpoint="https://example.com/token",
        redirect_uri=f"http://localhost:{ephemeral_port}/",
    )
    server = _OAuthCallbackServer(ephemeral_port, context)
    server.start()
    try:
        response = httpx.get(
            f"http://127.0.0.1:{ephemeral_port}/",
            params={
                "error": "invalid_request",
                "error_description": "Organization 'fff' not found (request ID: b1b0e0c4)",
                "state": "state",
            },
            timeout=10.0,
        )
    finally:
        server.stop()

    assert response.status_code == 200
    assert "Organization 'fff' not found" in response.text
    assert "Close this tab and return to the terminal." in response.text


def test_unsupported_session_version_is_not_loaded(cli_home: Path) -> None:
    (cli_home / "session.json").write_text(
        '{"version": 0, "org": "org", "accessTokenExpiresAt": "x", "refreshTokenExpiresAt": "y"}\n'
    )
    with pytest.raises(AuthenticationError, match="Unsupported session version"):
        read_session_metadata()


def test_missing_keyring_tokens_clears_stale_session_metadata(sample_keyring: Path, cli_home: Path) -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    write_session(
        StoredSession(
            version=COGNITE_CLI_SESSION_VERSION,
            org="my-org",
            access_token="access",
            refresh_token="refresh",
            access_token_expires_at=(now + timedelta(hours=1)).isoformat(),
            refresh_token_expires_at=(now + timedelta(hours=2)).isoformat(),
        )
    )
    delete_session_token("my-org/accessToken")
    delete_session_token("my-org/refreshToken")

    assert read_session() is None
    assert read_session_metadata() is None


def test_refresh_session_raises_when_idp_rejects_refresh_token(respx_mock: respx.MockRouter, cogidp_http) -> None:
    cogidp_http(token_status=400)
    session = StoredSession(
        version=COGNITE_CLI_SESSION_VERSION,
        org="my-org",
        access_token="access",
        refresh_token="refresh",
        access_token_expires_at="2026-01-01T01:00:00.000Z",
        refresh_token_expires_at="2026-01-02T01:00:00.000Z",
    )

    with pytest.raises(AuthenticationError, match="Session expired"):
        refresh_session_tokens(session)


def test_refresh_session_keeps_refresh_token_when_idp_omits_it(respx_mock: respx.MockRouter, cogidp_http) -> None:
    cogidp_http(token_json={"access_token": "new-access", "expires_in": 3600})
    session = StoredSession(
        version=COGNITE_CLI_SESSION_VERSION,
        org="my-org",
        access_token="access",
        refresh_token="refresh",
        access_token_expires_at="2026-01-01T01:00:00.000Z",
        refresh_token_expires_at="2026-01-02T01:00:00.000Z",
    )

    refreshed = refresh_session_tokens(session)

    assert refreshed.access_token == "new-access"
    assert refreshed.refresh_token == "refresh"


def test_ensure_fresh_session_refreshes_expiring_access_token(
    sample_keyring: Path, cli_home: Path, respx_mock: respx.MockRouter, cogidp_http
) -> None:
    cogidp_http(token_json={"access_token": "new-access", "refresh_token": "refresh", "expires_in": 3600})
    now = datetime.now(timezone.utc)
    write_session(
        StoredSession(
            version=COGNITE_CLI_SESSION_VERSION,
            org="my-org",
            access_token="access",
            refresh_token="refresh",
            access_token_expires_at=(now + timedelta(minutes=1)).isoformat(),
            refresh_token_expires_at=(now + timedelta(hours=2)).isoformat(),
        )
    )

    refreshed = ensure_fresh_session()

    assert refreshed is not None
    assert refreshed.access_token == "new-access"
    assert read_session() is not None
    assert read_session().access_token == "new-access"
