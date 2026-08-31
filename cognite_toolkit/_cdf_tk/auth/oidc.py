import secrets
import socket
import threading
import time
import webbrowser
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse

import httpx
from authlib.oauth2.rfc7636 import create_s256_code_challenge
from rich import print

from cognite_toolkit._cdf_tk.auth.session_store import StoredSession
from cognite_toolkit._cdf_tk.constants import (
    COGNITE_CLI_CLIENT_ID,
    COGNITE_CLI_DEFAULT_ACCESS_TOKEN_TTL_SECONDS,
    COGNITE_CLI_DEFAULT_CALLBACK_PORT,
    COGNITE_CLI_REFRESH_TOKEN_IDLE_TTL_SECONDS,
    COGNITE_CLI_SESSION_SCOPES,
    COGNITE_CLI_SESSION_VERSION,
    COGNITE_IDP_BASE_URL,
)
from cognite_toolkit._cdf_tk.exceptions import AuthenticationError

LOGIN_TIMEOUT_SECONDS = 5 * 60


@dataclass(frozen=True)
class OpenIdConfiguration:
    authorization_endpoint: str
    token_endpoint: str
    revocation_endpoint: str | None


@dataclass
class _CallbackContext:
    expected_state: str
    code_verifier: str
    token_endpoint: str
    redirect_uri: str
    result: dict[str, Any] | None = None


def resolve_idp_base_url() -> str:
    return COGNITE_IDP_BASE_URL


def callback_redirect_uri(port: int) -> str:
    return f"http://localhost:{port}/"


def fetch_openid_configuration(base_url: str | None = None) -> OpenIdConfiguration:
    idp_base = (base_url or resolve_idp_base_url()).rstrip("/")
    url = f"{idp_base}/.well-known/openid-configuration"
    try:
        response = httpx.get(url, timeout=30.0)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        raise AuthenticationError(f"Failed to fetch OpenID configuration from {idp_base}") from exc
    data = response.json()
    return OpenIdConfiguration(
        authorization_endpoint=data["authorization_endpoint"],
        token_endpoint=data["token_endpoint"],
        revocation_endpoint=data.get("revocation_endpoint"),
    )


def _as_seconds(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            value = int(value)
        except ValueError:
            return None
    if isinstance(value, (int, float)) and float(value) == value:
        return int(value)
    return None


def build_session_from_tokens(org: str, tokens: dict[str, Any], now: datetime | None = None) -> StoredSession:
    now = now or datetime.now(timezone.utc)
    access_token = tokens.get("access_token")
    if not access_token:
        raise AuthenticationError("Login succeeded but no access token was returned. Please try logging in again.")
    refresh_token = tokens.get("refresh_token")
    if not refresh_token:
        raise AuthenticationError(
            "Login succeeded but no refresh token was returned. "
            "The identity provider may not support the `offline_access` scope."
        )
    access_ttl = _as_seconds(tokens.get("expires_in")) or COGNITE_CLI_DEFAULT_ACCESS_TOKEN_TTL_SECONDS
    refresh_ttl = _as_seconds(tokens.get("refresh_expires_in")) or COGNITE_CLI_REFRESH_TOKEN_IDLE_TTL_SECONDS
    return StoredSession(
        version=COGNITE_CLI_SESSION_VERSION,
        org=org,
        access_token=access_token,
        refresh_token=refresh_token,
        access_token_expires_at=(now + timedelta(seconds=access_ttl)).isoformat(),
        refresh_token_expires_at=(now + timedelta(seconds=refresh_ttl)).isoformat(),
    )


def _generate_pkce_pair() -> tuple[str, str]:
    verifier = secrets.token_urlsafe(64)
    challenge = create_s256_code_challenge(verifier)
    return verifier, challenge


def _can_bind(host: str, port: int) -> bool:
    family = socket.AF_INET6 if ":" in host else socket.AF_INET
    with socket.socket(family, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((host, port))
        except OSError:
            return False
    return True


def _callback_loopback_hosts(port: int) -> tuple[str, ...]:
    if not _can_bind("127.0.0.1", port):
        raise AuthenticationError(f"Port {port} is already in use — the login callback server cannot start.")
    hosts: list[str] = ["127.0.0.1"]
    if _can_bind("::1", port):
        hosts.append("::1")
    return tuple(hosts)


def _make_callback_handler(context: _CallbackContext) -> type[BaseHTTPRequestHandler]:
    class CallbackHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            return

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path != "/":
                self.send_response(404)
                self.end_headers()
                self.wfile.write(b"Not found")
                return

            params = parse_qs(parsed.query)
            if "error" in params:
                error = params.get("error", ["unknown"])[0]
                description = params.get("error_description", [""])[0]
                context.result = {
                    "error": AuthenticationError(f"Authentication failed: {error}. {description}".strip())
                }
                self._respond_html("Authentication Error", str(context.result["error"]), success=False)
                return

            state = params.get("state", [""])[0]
            code = params.get("code", [""])[0]
            if state != context.expected_state or not code:
                context.result = {"error": AuthenticationError("Invalid OAuth callback state or missing code.")}
                self._respond_html("Authentication Error", "Invalid callback.", success=False)
                return

            print("Exchanging authorization code for tokens...")
            try:
                response = httpx.post(
                    context.token_endpoint,
                    data={
                        "grant_type": "authorization_code",
                        "client_id": COGNITE_CLI_CLIENT_ID,
                        "code": code,
                        "redirect_uri": context.redirect_uri,
                        "code_verifier": context.code_verifier,
                    },
                    headers={"Accept": "application/json"},
                    timeout=30.0,
                )
                response.raise_for_status()
                context.result = {"tokens": response.json()}
                self._respond_html(
                    "Toolkit login successful",
                    "You can close this window and return to the terminal.",
                    success=True,
                )
            except httpx.HTTPError as exc:
                context.result = {"error": AuthenticationError(f"Token exchange failed: {exc}")}
                self._respond_html("Authentication Error", str(exc), success=False)

        def _respond_html(self, title: str, message: str, *, success: bool) -> None:
            color = "#16a34a" if success else "#dc2626"
            body = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>{title}</title></head>
<body style="font-family: system-ui, sans-serif; padding: 2rem;">
<h1 style="color:{color};">{title}</h1><p>{message}</p>
</body></html>"""
            encoded = body.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(encoded)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(encoded)

    return CallbackHandler


def _create_loopback_server(host: str, port: int, handler: type[BaseHTTPRequestHandler]) -> ThreadingHTTPServer | None:
    try:
        if host == "::1":

            class IPv6ThreadingHTTPServer(ThreadingHTTPServer):
                address_family = socket.AF_INET6

            server: ThreadingHTTPServer = IPv6ThreadingHTTPServer(("::1", port), handler)
        else:
            server = ThreadingHTTPServer((host, port), handler)
        server.daemon_threads = True
        return server
    except OSError:
        return None


class _OAuthCallbackServer:
    def __init__(self, port: int, context: _CallbackContext) -> None:
        self._port = port
        self._context = context
        self._hosts = _callback_loopback_hosts(port)
        handler = _make_callback_handler(context)
        self._servers: list[ThreadingHTTPServer] = []
        self._threads: list[threading.Thread] = []
        for host in self._hosts:
            server = _create_loopback_server(host, port, handler)
            if server is not None:
                self._servers.append(server)

    def start(self) -> None:
        if not self._servers:
            raise AuthenticationError(f"Login callback server failed to start on port {self._port}.")
        for server in self._servers:
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            self._threads.append(thread)
        # Bind happens in server __init__; brief pause lets serve_forever threads enter accept().
        time.sleep(0.1)

    def wait_for_result(self, timeout_seconds: int) -> dict[str, Any] | None:
        deadline = time.monotonic() + timeout_seconds
        while self._context.result is None and time.monotonic() < deadline:
            time.sleep(0.1)
        return self._context.result

    def stop(self) -> None:
        for server in self._servers:
            server.shutdown()
            server.server_close()


def login_for_session(org: str, port: int | None = None) -> StoredSession:
    if not org.strip():
        raise AuthenticationError("Organization name is required.")
    org = org.strip()
    callback_port = port or COGNITE_CLI_DEFAULT_CALLBACK_PORT

    print("Starting CDF login flow...\n")
    idp_base = resolve_idp_base_url()
    print(f"Fetching OpenID configuration from {idp_base}...")
    oidc = fetch_openid_configuration(idp_base)

    code_verifier, code_challenge = _generate_pkce_pair()
    state = secrets.token_urlsafe(32)
    redirect_uri = callback_redirect_uri(callback_port)

    auth_params = {
        "client_id": COGNITE_CLI_CLIENT_ID,
        "response_type": "code",
        "scope": COGNITE_CLI_SESSION_SCOPES,
        "redirect_uri": redirect_uri,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
        "state": state,
        "organization_hint": org,
    }
    auth_url = f"{oidc.authorization_endpoint}?{urlencode(auth_params)}"

    context = _CallbackContext(
        expected_state=state,
        code_verifier=code_verifier,
        token_endpoint=oidc.token_endpoint,
        redirect_uri=redirect_uri,
    )
    callback_server = _OAuthCallbackServer(callback_port, context)
    callback_server.start()
    print(f"Local HTTP server started on http://localhost:{callback_port}")

    print(f"Organization: {org}")
    print("Opening browser for authentication...\n")

    try:
        webbrowser.open(auth_url)
    except Exception:
        print(f"Could not open browser automatically. Open this URL manually:\n{auth_url}\n")

    try:
        result = callback_server.wait_for_result(LOGIN_TIMEOUT_SECONDS)
    finally:
        callback_server.stop()

    if result is None:
        raise AuthenticationError("Login timed out. Please try again.")
    if "error" in result:
        raise result["error"]
    return build_session_from_tokens(org, result["tokens"])


def refresh_session_tokens(session: StoredSession) -> StoredSession:
    oidc = fetch_openid_configuration()
    try:
        response = httpx.post(
            oidc.token_endpoint,
            data={
                "grant_type": "refresh_token",
                "client_id": COGNITE_CLI_CLIENT_ID,
                "refresh_token": session.refresh_token,
            },
            headers={"Accept": "application/json"},
            timeout=30.0,
        )
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        body = exc.response.text
        if "invalid_grant" in body:
            raise AuthenticationError("Session expired. Run `cdf auth login` to sign in again.") from exc
        raise AuthenticationError(f"Token refresh failed: {exc}") from exc
    except httpx.HTTPError as exc:
        raise AuthenticationError(f"Token refresh failed: {exc}") from exc

    tokens = response.json()
    if not tokens.get("refresh_token"):
        tokens["refresh_token"] = session.refresh_token
    return build_session_from_tokens(session.org, tokens)


def revoke_refresh_token(refresh_token: str) -> None:
    oidc = fetch_openid_configuration()
    if not oidc.revocation_endpoint:
        return
    try:
        httpx.post(
            oidc.revocation_endpoint,
            data={"token": refresh_token, "client_id": COGNITE_CLI_CLIENT_ID},
            timeout=30.0,
        )
    except httpx.HTTPError:
        pass
