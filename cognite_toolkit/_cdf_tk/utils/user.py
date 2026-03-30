from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel

from cognite_toolkit._cdf_tk.client.resource_classes.principal import ServiceAccountPrincipal, UserPrincipal

if TYPE_CHECKING:
    from cognite_toolkit._cdf_tk.client import ToolkitClient


class UserInfo(BaseModel):
    """Best-effort representation of the calling CDF principal, used for Mixpanel tracking."""

    type: Literal["user", "service_account", "unknown"] = "unknown"
    id: str | None = None
    name: str | None = None
    email: str | None = None
    external_id: str | None = None

    @classmethod
    def load(cls, client: "ToolkitClient") -> "UserInfo":
        """Identify the calling principal on a best-effort basis for Mixpanel tracking.

        Tries in order:
        1. principals/me, which works for both users and service accounts.
        2. userProfile/me, which is a fallback when the principals API don't provide enough information.
        3. Returns an "unknown" UserInfo if both calls fail.
        """
        try:
            match client.principals.me():
                case UserPrincipal(id=id, name=name, email=email):
                    return cls(type="user", id=id, name=name, email=email)
                case ServiceAccountPrincipal(id=id, name=name, external_id=external_id):
                    return cls(type="service_account", id=id, name=name, external_id=external_id)
        except Exception:
            pass

        try:
            profile = client.user_profiles.me()
            return cls(type="user", id=profile.user_identifier, name=profile.display_name, email=profile.email)
        except Exception:
            pass

        return cls(type="unknown")
