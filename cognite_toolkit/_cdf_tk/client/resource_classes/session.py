"""Session resource classes for the Cognite Sessions API.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Sessions
"""

from typing import Literal, TypeAlias

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, RequestItem
from cognite_toolkit._cdf_tk.client.identifiers import InternalId

SessionType: TypeAlias = Literal["CLIENT_CREDENTIALS", "TOKEN_EXCHANGE", "ONESHOT_TOKEN_EXCHANGE"]
SessionStatus: TypeAlias = Literal["READY", "ACTIVE", "CANCELLED", "EXPIRED", "REVOKED", "ACCESS_LOST", "DETACHED"]


class ClientCredentialsSessionRequest(RequestItem):
    """Create a session with identity-provider client credentials."""

    client_id: str
    client_secret: str

    def __str__(self) -> str:
        return f"clientId={self.client_id}"


class TokenExchangeSessionRequest(RequestItem):
    """Create a session that reuses the caller's credentials via token exchange."""

    token_exchange: Literal[True] = True

    def __str__(self) -> str:
        return "tokenExchange"


class OneshotTokenExchangeSessionRequest(RequestItem):
    """Create a short-lived session that is not refreshed."""

    oneshot_token_exchange: Literal[True] = True

    def __str__(self) -> str:
        return "oneshotTokenExchange"


SessionCreateRequest: TypeAlias = (
    ClientCredentialsSessionRequest | TokenExchangeSessionRequest | OneshotTokenExchangeSessionRequest
)


class Session(BaseModelObject):
    """A CDF session.

    Create responses include ``nonce`` and omit creation and expiration times.
    List and retrieve responses include creation and expiration times and omit ``nonce``.
    Revoke responses always include ``id``. The remaining fields are present only when the
    caller has ``sessionsAcl:LIST``.
    """

    id: int
    type: SessionType | None = None
    status: SessionStatus | None = None
    nonce: str | None = None
    client_id: str | None = None
    creation_time: int | None = None
    expiration_time: int | None = None

    def as_id(self) -> InternalId:
        return InternalId(id=self.id)
