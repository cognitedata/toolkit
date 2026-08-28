import sys
import threading

import rust_native_keyring as rnk

from cognite_toolkit._cdf_tk.constants import COGNITE_CLI_KEYRING_SERVICE
from cognite_toolkit._cdf_tk.exceptions import AuthenticationError

_CHUNK_HEADER_PREFIX = "cognite-session/chunks="
_WINDOWS_CHUNK_SIZE = 1280

_store_lock = threading.Lock()
_store_name: str | None = None


def _platform_store_name() -> str:
    if sys.platform == "darwin":
        return "keychain"
    if sys.platform == "win32":
        return "windows"
    return "secret-service"


def _effective_chunk_size() -> int:
    return _WINDOWS_CHUNK_SIZE if sys.platform == "win32" else 2**31 - 1


def _chunk_account(account: str, index: int) -> str:
    return f"{account}/chunk/{index}"


def _parse_chunk_count(value: str) -> int | None:
    if not value.startswith(_CHUNK_HEADER_PREFIX):
        return None
    count = int(value.removeprefix(_CHUNK_HEADER_PREFIX))
    return count if count > 0 else None


def _ensure_store(store_name: str | None = None) -> None:
    global _store_name
    target = store_name or _platform_store_name()
    with _store_lock:
        if _store_name == target:
            return
        if _store_name is not None:
            rnk.release_store()
        rnk.use_named_store(target)
        _store_name = target


def configure_sample_store(backing_file: str) -> None:
    """Test helper: use a file-backed keyring store."""
    global _store_name
    with _store_lock:
        if _store_name is not None:
            rnk.release_store()
        rnk.use_named_store("sample", {"backing-file": backing_file})
        _store_name = "sample"


def reset_store() -> None:
    global _store_name
    with _store_lock:
        if _store_name is not None:
            rnk.release_store()
            _store_name = None


def _entry(account: str) -> rnk.Entry:
    _ensure_store()
    return rnk.Entry(COGNITE_CLI_KEYRING_SERVICE, account)


def _read_entry_password(account: str) -> str | None:
    try:
        return _entry(account).get_password()
    except RuntimeError:
        return None


def _delete_entry(account: str) -> None:
    try:
        _entry(account).delete_credential()
    except RuntimeError:
        pass


def store_session_token(account: str, value: str) -> None:
    chunk_size = _effective_chunk_size()
    main = _entry(account)
    previous = _read_entry_password(account)
    if previous is not None:
        old_chunk_count = _parse_chunk_count(previous)
        if old_chunk_count is not None:
            for index in range(old_chunk_count):
                _delete_entry(_chunk_account(account, index))

    try:
        if len(value) <= chunk_size:
            main.set_password(value)
            return

        chunks = [value[i : i + chunk_size] for i in range(0, len(value), chunk_size)]
        main.set_password(f"{_CHUNK_HEADER_PREFIX}{len(chunks)}")
        for index, chunk in enumerate(chunks):
            _entry(_chunk_account(account, index)).set_password(chunk)
    except RuntimeError as exc:
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
