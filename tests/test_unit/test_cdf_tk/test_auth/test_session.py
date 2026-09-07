import socket
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import httpx
import pytest

from cognite_toolkit._cdf_tk.auth.oidc import (
    OpenIdConfiguration,
    _callback_loopback_hosts,
    build_session_from_tokens,
    refresh_session_tokens,
)
from cognite_toolkit._cdf_tk.auth.session_refresh import ensure_fresh_session
from cognite_toolkit._cdf_tk.auth.session_store import (
    SessionMetadata,
    StoredSession,
    read_session,
    read_session_metadata,
    token_state,
    write_session,
)
from cognite_toolkit._cdf_tk.constants import COGNITE_CLI_SESSION_VERSION
from cognite_toolkit._cdf_tk.exceptions import AuthenticationError


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
    assert session.access_token == "access"
    assert session.refresh_token == "refresh"
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


def test_callback_loopback_hosts_includes_ipv6_when_available() -> None:
    with patch("cognite_toolkit._cdf_tk.auth.oidc._can_bind", side_effect=lambda host, port: True):
        assert _callback_loopback_hosts(3000) == ("127.0.0.1", "::1")


def test_callback_loopback_hosts_ipv4_only_when_ipv6_unavailable() -> None:
    with patch(
        "cognite_toolkit._cdf_tk.auth.oidc._can_bind",
        side_effect=lambda host, port: host == "127.0.0.1",
    ):
        assert _callback_loopback_hosts(3000) == ("127.0.0.1",)


def test_callback_server_accepts_localhost_connection() -> None:
    from cognite_toolkit._cdf_tk.auth.oidc import _CallbackContext, _OAuthCallbackServer

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    context = _CallbackContext(
        expected_state="state",
        code_verifier="verifier",
        token_endpoint="https://example.com/token",
        redirect_uri=f"http://localhost:{port}/",
    )
    server = _OAuthCallbackServer(port, context)
    server.start()
    try:
        with socket.create_connection(("localhost", port), timeout=2):
            pass
    finally:
        server.stop()


def test_read_session_metadata_version_mismatch(sample_keyring, tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    cli_home = tmp_path / ".cognite-cli"
    monkeypatch.setenv("COGNITE_CLI_HOME", str(cli_home))
    cli_home.mkdir()
    (cli_home / "session.json").write_text(
        '{"version": 0, "org": "org", "accessTokenExpiresAt": "x", "refreshTokenExpiresAt": "y"}\n'
    )
    with pytest.raises(AuthenticationError, match="Unsupported session version"):
        read_session_metadata()


def test_read_session_clears_metadata_when_tokens_missing(
    sample_keyring, tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cli_home = tmp_path / ".cognite-cli"
    monkeypatch.setenv("COGNITE_CLI_HOME", str(cli_home))
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
    from cognite_toolkit._cdf_tk.auth.session_keyring import delete_session_token

    delete_session_token("my-org/accessToken")
    delete_session_token("my-org/refreshToken")
    assert read_session() is None
    assert read_session_metadata() is None


def test_refresh_session_tokens_invalid_grant() -> None:
    session = StoredSession(
        version=COGNITE_CLI_SESSION_VERSION,
        org="my-org",
        access_token="access",
        refresh_token="refresh",
        access_token_expires_at="2026-01-01T01:00:00.000Z",
        refresh_token_expires_at="2026-01-02T01:00:00.000Z",
    )
    request = httpx.Request("POST", "https://example.com/token")

    with (
        patch(
            "cognite_toolkit._cdf_tk.auth.oidc.fetch_openid_configuration",
            return_value=OpenIdConfiguration("https://example.com/auth", "https://example.com/token", None),
        ),
        patch(
            "cognite_toolkit._cdf_tk.auth.oidc.httpx.post",
            return_value=httpx.Response(400, request=request, text='{"error":"invalid_grant"}'),
        ),
    ):
        with pytest.raises(AuthenticationError, match="Session expired"):
            refresh_session_tokens(session)


def test_refresh_session_tokens_keeps_refresh_token_when_omitted() -> None:
    session = StoredSession(
        version=COGNITE_CLI_SESSION_VERSION,
        org="my-org",
        access_token="access",
        refresh_token="refresh",
        access_token_expires_at="2026-01-01T01:00:00.000Z",
        refresh_token_expires_at="2026-01-02T01:00:00.000Z",
    )

    request = httpx.Request("POST", "https://example.com/token")

    with (
        patch(
            "cognite_toolkit._cdf_tk.auth.oidc.fetch_openid_configuration",
            return_value=OpenIdConfiguration("https://example.com/auth", "https://example.com/token", None),
        ),
        patch(
            "cognite_toolkit._cdf_tk.auth.oidc.httpx.post",
            return_value=httpx.Response(
                200,
                request=request,
                json={"access_token": "new-access", "expires_in": 3600},
            ),
        ),
    ):
        refreshed = refresh_session_tokens(session)
    assert refreshed.access_token == "new-access"
    assert refreshed.refresh_token == "refresh"


def test_ensure_fresh_session_refreshes_expiring_token(
    sample_keyring, tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cli_home = tmp_path / ".cognite-cli"
    monkeypatch.setenv("COGNITE_CLI_HOME", str(cli_home))
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
    request = httpx.Request("POST", "https://example.com/token")

    with (
        patch(
            "cognite_toolkit._cdf_tk.auth.oidc.fetch_openid_configuration",
            return_value=OpenIdConfiguration("https://example.com/auth", "https://example.com/token", None),
        ),
        patch(
            "cognite_toolkit._cdf_tk.auth.oidc.httpx.post",
            return_value=httpx.Response(
                200,
                request=request,
                json={"access_token": "new-access", "refresh_token": "refresh", "expires_in": 3600},
            ),
        ),
    ):
        refreshed = ensure_fresh_session()
    assert refreshed is not None
    assert refreshed.access_token == "new-access"
