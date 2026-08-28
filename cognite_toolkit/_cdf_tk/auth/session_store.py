import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal

from cognite_toolkit._cdf_tk.auth.home import get_cli_home, session_file_path
from cognite_toolkit._cdf_tk.auth.session_keyring import (
    delete_session_token,
    read_session_token,
    store_session_token,
)
from cognite_toolkit._cdf_tk.constants import (
    COGNITE_CLI_ACCESS_TOKEN_LEEWAY_SECONDS,
    COGNITE_CLI_SESSION_VERSION,
)
from cognite_toolkit._cdf_tk.exceptions import AuthenticationError

SessionTokenState = Literal["VALID", "EXPIRING", "EXPIRED", "MISSING"]


@dataclass
class SessionMetadata:
    version: int
    org: str
    access_token_expires_at: str
    refresh_token_expires_at: str


@dataclass
class StoredSession(SessionMetadata):
    access_token: str
    refresh_token: str


def _access_token_account(org: str) -> str:
    return f"{org}/accessToken"


def _refresh_token_account(org: str) -> str:
    return f"{org}/refreshToken"


def _parse_iso_timestamp(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def token_state(
    metadata: SessionMetadata,
    now: datetime | None = None,
    leeway_seconds: int = COGNITE_CLI_ACCESS_TOKEN_LEEWAY_SECONDS,
) -> SessionTokenState:
    now = now or datetime.now(timezone.utc)
    refresh_expires = _parse_iso_timestamp(metadata.refresh_token_expires_at)
    if now >= refresh_expires:
        return "EXPIRED"
    access_expires = _parse_iso_timestamp(metadata.access_token_expires_at)
    if now >= access_expires - timedelta(seconds=leeway_seconds):
        return "EXPIRING"
    return "VALID"


def _ensure_cli_home() -> Path:
    home = get_cli_home()
    home.mkdir(parents=True, exist_ok=True)
    os.chmod(home, 0o700)
    return home


def _write_metadata_file(metadata: SessionMetadata) -> None:
    _ensure_cli_home()
    path = session_file_path()
    payload = {
        "version": metadata.version,
        "org": metadata.org,
        "accessTokenExpiresAt": metadata.access_token_expires_at,
        "refreshTokenExpiresAt": metadata.refresh_token_expires_at,
    }
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    os.chmod(tmp, 0o600)
    tmp.replace(path)


def _read_metadata_file() -> SessionMetadata | None:
    path = session_file_path()
    if not path.is_file():
        return None
    raw: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    if raw.get("version") != COGNITE_CLI_SESSION_VERSION:
        raise AuthenticationError(
            f"Unsupported session version {raw.get('version')!r}. Run `cdf auth login --force` to sign in again."
        )
    return SessionMetadata(
        version=raw["version"],
        org=raw["org"],
        access_token_expires_at=raw["accessTokenExpiresAt"],
        refresh_token_expires_at=raw["refreshTokenExpiresAt"],
    )


def write_session(session: StoredSession) -> None:
    access_account = _access_token_account(session.org)
    refresh_account = _refresh_token_account(session.org)
    try:
        store_session_token(access_account, session.access_token)
        store_session_token(refresh_account, session.refresh_token)
        _write_metadata_file(session)
    except Exception:
        delete_session_token(access_account)
        delete_session_token(refresh_account)
        raise


def read_session_metadata() -> SessionMetadata | None:
    return _read_metadata_file()


def read_session() -> StoredSession | None:
    metadata = _read_metadata_file()
    if metadata is None:
        return None
    access_token = read_session_token(_access_token_account(metadata.org))
    refresh_token = read_session_token(_refresh_token_account(metadata.org))
    if not access_token or not refresh_token:
        raise AuthenticationError(
            "Session metadata exists but tokens are missing from the credential store. "
            "Run `cdf auth login` to sign in again."
        )
    return StoredSession(
        version=metadata.version,
        org=metadata.org,
        access_token_expires_at=metadata.access_token_expires_at,
        refresh_token_expires_at=metadata.refresh_token_expires_at,
        access_token=access_token,
        refresh_token=refresh_token,
    )


def clear_session() -> None:
    metadata = _read_metadata_file()
    if metadata is not None:
        delete_session_token(_access_token_account(metadata.org))
        delete_session_token(_refresh_token_account(metadata.org))
    path = session_file_path()
    if path.is_file():
        path.unlink()
