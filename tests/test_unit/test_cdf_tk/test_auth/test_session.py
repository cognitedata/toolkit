import socket
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from cognite_toolkit._cdf_tk.auth.oidc import _callback_loopback_hosts, build_session_from_tokens
from cognite_toolkit._cdf_tk.auth.session_store import SessionMetadata, token_state
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
    assert session.access_token_expires_at.startswith("2026-01-01T01:00:00")


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

    port = 3011
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
