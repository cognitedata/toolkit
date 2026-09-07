import sys

import keyring
from keyring.backend import KeyringBackend
from keyring.errors import PasswordDeleteError

from cognite_toolkit._cdf_tk.constants import COGNITE_CLI_KEYRING_SERVICE
from cognite_toolkit._cdf_tk.exceptions import AuthenticationError

_CHUNK_HEADER_PREFIX = "cognite-session/chunks="
_WINDOWS_CHUNK_SIZE = 1280

_default_keyring = keyring.get_keyring()
_active_keyring: KeyringBackend | None = None


def _effective_chunk_size() -> int:
    return _WINDOWS_CHUNK_SIZE if sys.platform == "win32" else 2**31 - 1


def _chunk_account(account: str, index: int) -> str:
    return f"{account}/chunk/{index}"


def _parse_chunk_count(value: str) -> int | None:
    if not value.startswith(_CHUNK_HEADER_PREFIX):
        return None
    try:
        count = int(value.removeprefix(_CHUNK_HEADER_PREFIX))
        return count if count > 0 else None
    except ValueError:
        return None


def configure_sample_store(backing_file: str) -> None:
    """Test helper: use an in-memory keyring backend."""
    global _active_keyring
    stored: dict[tuple[str, str], str] = {}

    class MemoryKeyring(KeyringBackend):
        priority = 0

        def set_password(self, service: str, username: str, password: str) -> None:
            stored[(service, username)] = password

        def get_password(self, service: str, username: str) -> str | None:
            return stored.get((service, username))

        def delete_password(self, service: str, username: str) -> None:
            stored.pop((service, username), None)

    _active_keyring = MemoryKeyring()
    keyring.set_keyring(_active_keyring)
    _ = backing_file  # kept for test API compatibility


def reset_store() -> None:
    global _active_keyring
    if _active_keyring is not None:
        keyring.set_keyring(_default_keyring)
        _active_keyring = None


def _read_entry_password(account: str) -> str | None:
    return keyring.get_password(COGNITE_CLI_KEYRING_SERVICE, account)


def _delete_entry(account: str) -> None:
    try:
        keyring.delete_password(COGNITE_CLI_KEYRING_SERVICE, account)
    except PasswordDeleteError:
        pass


def store_session_token(account: str, value: str) -> None:
    chunk_size = _effective_chunk_size()
    previous = _read_entry_password(account)
    if previous is not None:
        old_chunk_count = _parse_chunk_count(previous)
        if old_chunk_count is not None:
            for index in range(old_chunk_count):
                _delete_entry(_chunk_account(account, index))

    try:
        if len(value) <= chunk_size:
            keyring.set_password(COGNITE_CLI_KEYRING_SERVICE, account, value)
            return

        chunks = [value[i : i + chunk_size] for i in range(0, len(value), chunk_size)]
        keyring.set_password(COGNITE_CLI_KEYRING_SERVICE, account, f"{_CHUNK_HEADER_PREFIX}{len(chunks)}")
        for index, chunk in enumerate(chunks):
            keyring.set_password(COGNITE_CLI_KEYRING_SERVICE, _chunk_account(account, index), chunk)
    except Exception as exc:
        raise AuthenticationError(
            "Login succeeded but tokens could not be saved to the credential store. "
            "Ensure your OS keychain is available and unlocked, then run `cdf auth login` again."
        ) from exc


def read_session_token(account: str) -> str | None:
    value = _read_entry_password(account)
    if value is None:
        return None

    chunk_count = _parse_chunk_count(value)
    if chunk_count is None:
        return value

    parts: list[str] = []
    for index in range(chunk_count):
        chunk = _read_entry_password(_chunk_account(account, index))
        if chunk is None:
            return None
        parts.append(chunk)
    return "".join(parts)


def delete_session_token(account: str) -> None:
    header = _read_entry_password(account)
    if header is not None:
        chunk_count = _parse_chunk_count(header)
        if chunk_count is not None:
            for index in range(chunk_count):
                _delete_entry(_chunk_account(account, index))
    _delete_entry(account)
