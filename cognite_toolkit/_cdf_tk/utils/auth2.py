from typing import Literal, TypeAlias

from cognite_toolkit._version import __version__

LoginFlow: TypeAlias = Literal["client_credentials", "token", "device_code", "interactive"]
Provider: TypeAlias = Literal["entra_id", "cdf", "other"]

CLIENT_NAME = f"CDF-Toolkit:{__version__}"
LOGIN_FLOW_DESCRIPTION = {
    "client_credentials": "Setup a service principal with client credentials",
    "interactive": "Login using the browser with your user credentials",
    "device_code": "Login using the browser with your user credentials using device code flow",
    "token": "Use a Token directly to authenticate",
}

PROVIDER_DESCRIPTION = {
    "entra_id": "Use Microsoft Entra ID to authenticate",
    "cdf": "Use Cognite IDP to authenticate",
    "other": "Use other IDP to authenticate",
}
