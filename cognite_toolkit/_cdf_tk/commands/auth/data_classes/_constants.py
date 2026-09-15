import difflib

from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._version import __version__

from ._types import LoginFlow, Provider

CLIENT_NAME = f"CDF-Toolkit:{__version__}"

PROVIDERS: dict[Provider, str] = {
    "entra_id": "Use Microsoft Entra ID to authenticate",
    "auth0": "Use Auth0 to authenticate",
    "cdf": "Use Cognite IDP to authenticate",
    "other": "Use other IDP to authenticate",
}
LOGIN_FLOWS: dict[LoginFlow, str] = {
    "session": "Sign in via CogIdP and persist a refreshable session (recommended for local development)",
    "device_code": "Sign in via a browser on any device — no service principal needed (best for local setup)",
    "interactive": "Sign in via the browser on this machine with your user credentials",
    "client_credentials": "Use a service principal with client ID and secret (for CI/CD or automated workloads)",
    "token": "Supply a pre-existing token directly",
}


def parse_login_flow(flow: str) -> LoginFlow:
    normalized = flow.strip()
    if normalized in LOGIN_FLOWS:
        return normalized
    matches = difflib.get_close_matches(normalized, LOGIN_FLOWS, n=1)
    hint = f"Did you mean {matches[0]!r}?" if matches else f"Choose one of: {humanize_collection(LOGIN_FLOWS)}"
    raise ValueError(f"Invalid login flow: {flow!r}. {hint}")
