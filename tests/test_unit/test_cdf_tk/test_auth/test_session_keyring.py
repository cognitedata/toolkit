from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.auth.session_keyring import (
    configure_sample_store,
    delete_session_token,
    read_session_token,
    reset_store,
    store_session_token,
)
from cognite_toolkit._cdf_tk.auth.session_store import StoredSession, clear_session, read_session, write_session
from cognite_toolkit._cdf_tk.constants import COGNITE_CLI_SESSION_VERSION


@pytest.fixture
def sample_keyring(tmp_path: Path):
    backing_file = tmp_path / "keyring.ron"
    configure_sample_store(str(backing_file))
    yield backing_file
    reset_store()


def test_session_token_roundtrip(sample_keyring: Path) -> None:
    store_session_token("my-org/accessToken", "secret-token")
    assert read_session_token("my-org/accessToken") == "secret-token"
    delete_session_token("my-org/accessToken")
    assert read_session_token("my-org/accessToken") is None


def test_session_token_chunking(monkeypatch: pytest.MonkeyPatch, sample_keyring: Path) -> None:
    monkeypatch.setattr(
        "cognite_toolkit._cdf_tk.auth.session_keyring._effective_chunk_size",
        lambda: 10,
    )
    token = "abcdefghijklmnopqrstuvwxyz"
    store_session_token("my-org/refreshToken", token)
    assert read_session_token("my-org/refreshToken") == token


def test_write_and_read_session(sample_keyring: Path, tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    cli_home = tmp_path / ".cognite-cli"
    monkeypatch.setenv("COGNITE_CLI_HOME", str(cli_home))

    session = StoredSession(
        version=COGNITE_CLI_SESSION_VERSION,
        org="my-org",
        access_token="access",
        refresh_token="refresh",
        access_token_expires_at="2026-01-01T01:00:00+00:00",
        refresh_token_expires_at="2026-01-02T01:00:00+00:00",
    )
    write_session(session)
    loaded = read_session()
    assert loaded == session
    clear_session()
    assert read_session() is None
