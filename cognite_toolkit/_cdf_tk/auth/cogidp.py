import base64
import json
from dataclasses import dataclass
from urllib.parse import urlparse

import httpx

from cognite_toolkit._cdf_tk.constants import COGNITE_IDP_BASE_URL
from cognite_toolkit._cdf_tk.exceptions import AuthenticationError


@dataclass(frozen=True)
class SessionProject:
    name: str
    cluster: str | None = None
    is_default: bool = False


@dataclass(frozen=True)
class SessionUserInfo:
    sub: str
    email: str | None
    name: str | None
    preferred_username: str | None
    projects: list[SessionProject]


def _decode_jwt_claims(token: str) -> dict[str, str]:
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return {}
        payload = parts[1]
        padding = "=" * (-len(payload) % 4)
        decoded = base64.urlsafe_b64decode(payload + padding)
        data = json.loads(decoded)
        if not isinstance(data, dict):
            return {}
        return {k: v for k, v in data.items() if isinstance(v, str)}
    except (json.JSONDecodeError, ValueError):
        return {}


def _cluster_from_api_url(api_url: str | None) -> str | None:
    if not api_url:
        return None
    try:
        host = urlparse(api_url).hostname
        if host and host.endswith(".cognitedata.com"):
            return host.removesuffix(".cognitedata.com")
        return host
    except ValueError:
        return None


def fetch_session_user_info(org: str, access_token: str) -> SessionUserInfo:
    url = f"{COGNITE_IDP_BASE_URL}/api/v0/orgs/{org}/projects"
    try:
        response = httpx.get(
            url,
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=30.0,
        )
        response.raise_for_status()
    except httpx.HTTPError as exc:
        raise AuthenticationError(f"Failed to verify session with CogIdP: {exc}") from exc

    data = response.json()
    items = data.get("items", []) if isinstance(data, dict) else []
    projects: list[SessionProject] = []
    for item in items:
        if not isinstance(item, dict) or "name" not in item:
            continue
        projects.append(
            SessionProject(
                name=item["name"],
                cluster=_cluster_from_api_url(item.get("apiUrl")),
                is_default=bool(item.get("isDefault")),
            )
        )

    claims = _decode_jwt_claims(access_token)
    return SessionUserInfo(
        sub=claims.get("sub", "unknown"),
        email=claims.get("email"),
        name=claims.get("name"),
        preferred_username=claims.get("preferred_username"),
        projects=projects,
    )
