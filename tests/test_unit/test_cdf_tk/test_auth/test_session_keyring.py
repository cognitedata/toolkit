from pathlib import Path

import keyring
import pytest

from cognite_toolkit._cdf_tk.commands.auth.session_keyring import (
    delete_session_token,
    read_session_token,
    store_session_token,
)
from cognite_toolkit._cdf_tk.constants import COGNITE_CLI_KEYRING_SERVICE


def test_session_token_roundtrip(sample_keyring: Path) -> None:
    store_session_token("my-org/accessToken", "secret-token")
    assert read_session_token("my-org/accessToken") == "secret-token"
    delete_session_token("my-org/accessToken")
    assert read_session_token("my-org/accessToken") is None


def test_session_token_chunking(monkeypatch: pytest.MonkeyPatch, sample_keyring: Path) -> None:
    monkeypatch.setattr(
        "cognite_toolkit._cdf_tk.commands.auth.session_keyring._effective_chunk_size",
        lambda: 10,
    )
    token = "abcdefghijklmnopqrstuvwxyz"
    account = "my-org/refreshToken"
    store_session_token(account, token)

    assert read_session_token(account) == token
    assert keyring.get_password(COGNITE_CLI_KEYRING_SERVICE, account) == "cognite-session/chunks=3"
    assert keyring.get_password(COGNITE_CLI_KEYRING_SERVICE, f"{account}/chunk/0") == "abcdefghij"
