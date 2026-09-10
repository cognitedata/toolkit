from filelock import FileLock, Timeout

from cognite_toolkit._cdf_tk.exceptions import AuthenticationError

from .home import session_file_path
from .oidc import refresh_session_tokens
from .session_store import (
    StoredSession,
    read_session,
    token_state,
    write_session,
)


class SessionExpiredError(AuthenticationError):
    """Raised when the persisted session can no longer be refreshed."""


def ensure_fresh_session() -> StoredSession | None:
    session = read_session()
    if session is None:
        return None

    state = token_state(session)
    if state == "EXPIRED":
        raise SessionExpiredError("Session expired. Run `cdf auth login` to sign in again.")
    if state == "VALID":
        return session

    lock_path = session_file_path().with_suffix(".lock")
    try:
        with FileLock(lock_path, timeout=30):
            latest = read_session()
            if latest is None:
                return None
            latest_state = token_state(latest)
            if latest_state == "EXPIRED":
                raise SessionExpiredError("Session expired. Run `cdf auth login` to sign in again.")
            if latest_state == "VALID":
                return latest
            try:
                refreshed = refresh_session_tokens(latest)
            except AuthenticationError:
                raise SessionExpiredError("Session expired. Run `cdf auth login` to sign in again.") from None
            write_session(refreshed)
            return refreshed
    except Timeout as exc:
        raise AuthenticationError(
            "Timed out waiting for session lock. Another process might be refreshing the session."
        ) from exc
