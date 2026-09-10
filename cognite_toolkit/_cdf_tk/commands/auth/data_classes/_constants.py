from typing import get_args

from cognite_toolkit._version import __version__
from cognite_toolkit._cdf_tk.utils import humanize_collection

from ._types import LoginFlow, Provider

VALID_PROVIDERS = get_args(Provider)
VALID_LOGIN_FLOWS = get_args(LoginFlow)

_LOGIN_FLOW_BY_COMPACT_KEY: dict[str, LoginFlow] = {
    "clientcredentials": "client_credentials",
    "devicecode": "device_code",
    "interactive": "interactive",
    "session": "session",
    "token": "token",
}


def parse_login_flow(flow: str) -> LoginFlow:
    compact_key = flow.strip().lower().replace("-", "").replace("_", "")
    try:
        return _LOGIN_FLOW_BY_COMPACT_KEY[compact_key]
    except KeyError:
        raise ValueError(
            f"Invalid login flow: {flow!r}. Choose one of: {humanize_collection(VALID_LOGIN_FLOWS)}"
        ) from None

CLIENT_NAME = f"CDF-Toolkit:{__version__}"

PROVIDER_DESCRIPTION = {
    "entra_id": "Use Microsoft Entra ID to authenticate",
    "auth0": "Use Auth0 to authenticate",
    "cdf": "Use Cognite IDP to authenticate",
    "other": "Use other IDP to authenticate",
}
LOGIN_FLOW_DESCRIPTION = {
    "session": "Sign in via CogIdP and persist a refreshable session (recommended for local development)",
    "device_code": "Sign in via a browser on any device — no service principal needed (best for local setup)",
    "interactive": "Sign in via the browser on this machine with your user credentials",
    "client_credentials": "Use a service principal with client ID and secret (for CI/CD or automated workloads)",
    "token": "Supply a pre-existing token directly",
}
