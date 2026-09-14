from __future__ import annotations

import socket
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest
import respx

from cognite_toolkit import _cdf
from cognite_toolkit._cdf_tk import cdf_toml
from cognite_toolkit._cdf_tk.commands.auth.session_keyring import configure_sample_store, reset_store
from cognite_toolkit._cdf_tk.feature_flags import FeatureFlag


@pytest.fixture
def sample_keyring(tmp_path: Path) -> Iterator[Path]:
    backing_file = tmp_path / "keyring.ron"
    configure_sample_store(str(backing_file))
    yield backing_file
    reset_store()


@pytest.fixture
def cli_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    home = tmp_path / ".cognite-cli"
    home.mkdir()
    monkeypatch.setenv("COGNITE_CLI_HOME", str(home))
    return home


@pytest.fixture
def ephemeral_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture
def cogidp_http(respx_mock: respx.MockRouter):
    """Stub CogIdP OpenID discovery and token endpoint for session login tests."""

    def configure(
        base_url: str = "https://auth.cognite.com",
        *,
        token_status: int = 200,
        token_json: dict[str, object] | None = None,
    ) -> tuple[str, str]:
        respx_mock.route(host="127.0.0.1").pass_through()
        token_endpoint = f"{base_url}/token"
        respx_mock.get(f"{base_url}/.well-known/openid-configuration").mock(
            return_value=httpx.Response(
                status_code=200,
                json={
                    "authorization_endpoint": f"{base_url}/authorize",
                    "token_endpoint": token_endpoint,
                    "revocation_endpoint": f"{base_url}/revoke",
                },
            )
        )
        if token_status == 200:
            body = token_json or {
                "access_token": "access-token",
                "refresh_token": "refresh-token",
                "expires_in": 3600,
            }
            respx_mock.post(token_endpoint).mock(return_value=httpx.Response(status_code=200, json=body))
        else:
            respx_mock.post(token_endpoint).mock(
                return_value=httpx.Response(status_code=token_status, text='{"error":"invalid_grant"}')
            )
        return base_url, token_endpoint

    return configure


@pytest.fixture
def reset_cdf_toml_singleton():
    """Reset CDFToml singleton before and after each test to ensure test isolation.

    Use this fixture in tests that need to load cdf.toml from a test directory
    to avoid conflicts with the repo's cdf.toml singleton.
    """
    cdf_toml._CDF_TOML = None
    _cdf.CDF_TOML = None  # Also reset the module-level instance in _cdf.py
    FeatureFlag.flush()  # Also clear the feature flag cache
    yield
    cdf_toml._CDF_TOML = None
    _cdf.CDF_TOML = None
    FeatureFlag.flush()
