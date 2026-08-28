import threading

from cognite.client.credentials import Token

from cognite_toolkit._cdf_tk.auth.session_refresh import SessionExpiredError, ensure_fresh_session


class PersistedSessionCredentials(Token):
    """Credential provider backed by a persisted CogIdP session in ~/.cognite-cli/."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        super().__init__(self._get_token)

    def _get_token(self) -> str:
        with self._lock:
            session = ensure_fresh_session()
            if session is None:
                raise SessionExpiredError("Not signed in. Run `cdf auth login` to sign in.")
            return session.access_token
