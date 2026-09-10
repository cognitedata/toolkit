import socket
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import httpx
import pytest

from cognite_toolkit._cdf_tk.commands.auth.oidc import (
    OpenIdConfiguration,
    _callback_loopback_hosts,
    build_session_from_tokens,
    refresh_session_tokens,
)
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


def test_resolve_client_id_matches_cognite_cli() -> None:
    from cognite_toolkit._cdf_tk.commands.auth.oidc import (
        _DEV_CLIENT_ID,
        _PROD_CLIENT_ID,
        _resolve_client_id,
    )

    assert _resolve_client_id("cog-hyperion") == _PROD_CLIENT_ID
    assert _resolve_client_id("cog-dev-hyperion") == _DEV_CLIENT_ID


def test_resolve_idp_base_url_matches_cognite_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    from cognite_toolkit._cdf_tk.commands.auth.oidc import (
        _DEV_IDP_BASE_URL,
        _PROD_IDP_BASE_URL,
        _resolve_idp_base_url,
    )

    monkeypatch.delenv("COGNITE_IDP_BASE_URL", raising=False)
    assert _resolve_idp_base_url("cog-hyperion") == _PROD_IDP_BASE_URL
    assert _resolve_idp_base_url("cog-dev-hyperion") == _DEV_IDP_BASE_URL

    monkeypatch.setenv("COGNITE_IDP_BASE_URL", "https://auth.example.com/")
    assert _resolve_idp_base_url("cog-dev-hyperion") == "https://auth.example.com"


def test_login_prints_manual_url_when_browser_does_not_open(capsys) -> None:
    from cognite_toolkit._cdf_tk.commands.auth.oidc import _login_for_session_at_port

    with (
        patch(
            "cognite_toolkit._cdf_tk.commands.auth.oidc.fetch_openid_configuration",
            return_value=OpenIdConfiguration(
                "https://auth.example.com/authorize",
                "https://auth.example.com/token",
                None,
            ),
        ),
        patch("cognite_toolkit._cdf_tk.commands.auth.oidc.webbrowser.open", return_value=False),
        patch("cognite_toolkit._cdf_tk.commands.auth.oidc._OAuthCallbackServer") as server_cls,
    ):
        server_cls.return_value.wait_for_result.return_value = {
            "tokens": {"access_token": "access", "refresh_token": "refresh", "expires_in": 3600}
        }
        _login_for_session_at_port("my-org", 3000)

    captured = capsys.readouterr()
    assert "Could not open browser automatically" in captured.out
    assert "https://auth.example.com/authorize?" in captured.out


def test_login_prints_manual_url_when_browser_open_raises(capsys) -> None:
    from cognite_toolkit._cdf_tk.commands.auth.oidc import _login_for_session_at_port

    with (
        patch(
            "cognite_toolkit._cdf_tk.commands.auth.oidc.fetch_openid_configuration",
            return_value=OpenIdConfiguration(
                "https://auth.example.com/authorize",
                "https://auth.example.com/token",
                None,
            ),
        ),
        patch("cognite_toolkit._cdf_tk.commands.auth.oidc.webbrowser.open", side_effect=OSError("no browser")),
        patch("cognite_toolkit._cdf_tk.commands.auth.oidc._OAuthCallbackServer") as server_cls,
    ):
        server_cls.return_value.wait_for_result.return_value = {
            "tokens": {"access_token": "access", "refresh_token": "refresh", "expires_in": 3600}
        }
        _login_for_session_at_port("my-org", 3000)

    captured = capsys.readouterr()
    assert "Could not open browser automatically" in captured.out
    assert "https://auth.example.com/authorize?" in captured.out


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
    with patch("cognite_toolkit._cdf_tk.commands.auth.oidc._can_bind", side_effect=lambda host, port: True):
        assert _callback_loopback_hosts(3000) == ("127.0.0.1", "::1")


def test_callback_loopback_hosts_ipv4_only_when_ipv6_unavailable() -> None:
    with patch(
        "cognite_toolkit._cdf_tk.commands.auth.oidc._can_bind",
        side_effect=lambda host, port: host == "127.0.0.1",
    ):
        assert _callback_loopback_hosts(3000) == ("127.0.0.1",)


def test_callback_server_returns_oauth_error_from_url() -> None:
    from cognite_toolkit._cdf_tk.commands.auth.oidc import _CallbackContext, _OAuthCallbackServer

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    context = _CallbackContext(
        expected_state="state",
        code_verifier="verifier",
        client_id="test-client",
        token_endpoint="https://example.com/token",
        redirect_uri=f"http://localhost:{port}/",
    )
    server = _OAuthCallbackServer(port, context)
    server.start()
    try:
        with httpx.Client() as client:
            response = client.get(
                f"http://127.0.0.1:{port}/",
                params={
                    "error": "invalid_request",
                    "error_description": "Organization 'fff' not found (request ID: b1b0e0c4)",
                    "state": "state",
                },
            )
    finally:
        server.stop()

    assert response.status_code == 200
    assert "Organization 'fff' not found" in response.text
    assert "Close this tab and return to the terminal." in response.text
    assert context.result is not None
    assert "error" in context.result
    assert str(context.result["error"]) == "Organization 'fff' not found (request ID: b1b0e0c4)"


def test_callback_server_returns_plain_text_on_success() -> None:
    from cognite_toolkit._cdf_tk.commands.auth.oidc import _CallbackContext, _OAuthCallbackServer

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    context = _CallbackContext(
        expected_state="state",
        code_verifier="verifier",
        client_id="test-client",
        token_endpoint="https://example.com/token",
        redirect_uri=f"http://localhost:{port}/",
    )
    server = _OAuthCallbackServer(port, context)
    server.start()
    request = httpx.Request("POST", "https://example.com/token")
    try:
        with (
            patch(
                "cognite_toolkit._cdf_tk.commands.auth.oidc.httpx.post",
                return_value=httpx.Response(
                    200,
                    request=request,
                    json={"access_token": "access", "refresh_token": "refresh", "expires_in": 3600},
                ),
            ),
            httpx.Client() as client,
        ):
            response = client.get(f"http://127.0.0.1:{port}/", params={"state": "state", "code": "code"})
    finally:
        server.stop()

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert response.text == "Signed in. Close this tab and return to the terminal."
    assert context.result == {"tokens": {"access_token": "access", "refresh_token": "refresh", "expires_in": 3600}}


def test_callback_server_accepts_localhost_connection() -> None:
    from cognite_toolkit._cdf_tk.commands.auth.oidc import _CallbackContext, _OAuthCallbackServer

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    context = _CallbackContext(
        expected_state="state",
        code_verifier="verifier",
        client_id="test-client",
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
    from cognite_toolkit._cdf_tk.commands.auth.session_keyring import delete_session_token

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
            "cognite_toolkit._cdf_tk.commands.auth.oidc.fetch_openid_configuration",
            return_value=OpenIdConfiguration("https://example.com/auth", "https://example.com/token", None),
        ),
        patch(
            "cognite_toolkit._cdf_tk.commands.auth.oidc.httpx.post",
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
            "cognite_toolkit._cdf_tk.commands.auth.oidc.fetch_openid_configuration",
            return_value=OpenIdConfiguration("https://example.com/auth", "https://example.com/token", None),
        ),
        patch(
            "cognite_toolkit._cdf_tk.commands.auth.oidc.httpx.post",
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
            "cognite_toolkit._cdf_tk.commands.auth.oidc.fetch_openid_configuration",
            return_value=OpenIdConfiguration("https://example.com/auth", "https://example.com/token", None),
        ),
        patch(
            "cognite_toolkit._cdf_tk.commands.auth.oidc.httpx.post",
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
