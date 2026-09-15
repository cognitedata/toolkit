import json
import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Literal

from filelock import FileLock, Timeout

from cognite_toolkit._cdf_tk.commands.auth.home import get_cli_home, session_file_path
from cognite_toolkit._cdf_tk.commands.auth.session_keyring import (
    delete_session_token,
    read_session_token,
    store_session_token,
)
from cognite_toolkit._cdf_tk.constants import (
    COGNITE_CLI_ACCESS_TOKEN_LEEWAY_SECONDS,
    COGNITE_CLI_SESSION_VERSION,
)
from cognite_toolkit._cdf_tk.exceptions import AuthenticationError, SessionExpiredError

SessionTokenState = Literal["VALID", "EXPIRING", "EXPIRED"]


@dataclass
class SessionMetadata:
    version: int
    org: str
    access_token_expires_at: str
    refresh_token_expires_at: str

    def token_state(
        self,
        now: datetime | None = None,
        leeway_seconds: int = COGNITE_CLI_ACCESS_TOKEN_LEEWAY_SECONDS,
    ) -> SessionTokenState:
        now = now or datetime.now(timezone.utc)
        refresh_expires = _parse_iso_timestamp(self.refresh_token_expires_at)
        if now >= refresh_expires:
            return "EXPIRED"
        access_expires = _parse_iso_timestamp(self.access_token_expires_at)
        if now >= access_expires - timedelta(seconds=leeway_seconds):
            return "EXPIRING"
        return "VALID"


@dataclass
class StoredSession(SessionMetadata):
    access_token: str
    refresh_token: str

    @classmethod
    def load_metadata(cls) -> SessionMetadata | None:
        return _read_metadata_file()

    @classmethod
    def load(cls) -> "StoredSession | None":
        metadata = cls.load_metadata()
        if metadata is None:
            return None
        access_token = read_session_token(_access_token_account(metadata.org))
        refresh_token = read_session_token(_refresh_token_account(metadata.org))
        if not access_token or not refresh_token:
            cls.clear()
            return None
        return cls(
            version=metadata.version,
            org=metadata.org,
            access_token_expires_at=metadata.access_token_expires_at,
            refresh_token_expires_at=metadata.refresh_token_expires_at,
            access_token=access_token,
            refresh_token=refresh_token,
        )

    def save(self) -> None:
        try:
            existing = self.load_metadata()
        except AuthenticationError:
            existing = None

        if existing is not None and existing.org != self.org:
            self._clear_org_tokens(existing.org)

        access_account = _access_token_account(self.org)
        refresh_account = _refresh_token_account(self.org)
        try:
            store_session_token(access_account, self.access_token)
            store_session_token(refresh_account, self.refresh_token)
            _write_metadata_file(self)
        except Exception:
            delete_session_token(access_account)
            delete_session_token(refresh_account)
            raise

    @classmethod
    def clear(cls) -> None:
        try:
            metadata = cls.load_metadata()
        except AuthenticationError:
            metadata = None
        if metadata is not None:
            cls._clear_org_tokens(metadata.org)
        path = session_file_path()
        if path.is_file():
            path.unlink()

    @staticmethod
    def _clear_org_tokens(org: str) -> None:
        delete_session_token(_access_token_account(org))
        delete_session_token(_refresh_token_account(org))

    @classmethod
    def ensure_fresh(cls, refresh: Callable[["StoredSession"], "StoredSession"]) -> "StoredSession | None":
        session = cls.load()
        if session is None:
            return None

        state = session.token_state()
        if state == "EXPIRED":
            raise SessionExpiredError("Session expired. Run `cdf auth login` to sign in again.")
        if state == "VALID":
            return session

        lock_path = session_file_path().with_suffix(".lock")
        try:
            with FileLock(lock_path, timeout=30):
                latest = cls.load()
                if latest is None:
                    return None
                latest_state = latest.token_state()
                if latest_state == "EXPIRED":
                    raise SessionExpiredError("Session expired. Run `cdf auth login` to sign in again.")
                if latest_state == "VALID":
                    return latest
                try:
                    refreshed = refresh(latest)
                except AuthenticationError:
                    raise SessionExpiredError("Session expired. Run `cdf auth login` to sign in again.") from None
                refreshed.save()
                return refreshed
        except Timeout as exc:
            raise AuthenticationError(
                "Timed out waiting for session lock. Another process might be refreshing the session."
            ) from exc


def _access_token_account(org: str) -> str:
    return f"{org}/accessToken"


def _refresh_token_account(org: str) -> str:
    return f"{org}/refreshToken"


def format_session_timestamp(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    milliseconds = dt.microsecond // 1000
    return dt.strftime(f"%Y-%m-%dT%H:%M:%S.{milliseconds:03d}Z")


def _parse_iso_timestamp(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


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
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("Session file content is not a JSON object.")
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
    except AuthenticationError:
        raise
    except (json.JSONDecodeError, KeyError, ValueError, TypeError) as exc:
        raise AuthenticationError(
            f"Session file is corrupted or invalid: {exc}. Run `cdf auth login --force` to sign in again."
        ) from exc
