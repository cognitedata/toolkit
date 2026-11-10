"""Resource classes for the Postgres Gateway Users API.

Based on https://api-docs.cognite.com/20230101/tag/Postgres-Gateway-Users
"""

from typing import ClassVar

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
    UpdatableRequestResource,
)
from cognite_toolkit._cdf_tk.client.identifiers import PostgresGatewayUserId


class SessionCredentials(BaseModelObject):
    """Credentials for authenticating towards CDF using a CDF session."""

    nonce: str


class PostgresGatewayUserRequest(UpdatableRequestResource):
    """Request resource for creating and updating postgres gateway users.

    The API generates the username on create, so ``username`` is only sent on update.
    """

    non_nullable_fields: ClassVar[frozenset[str]] = frozenset({"credentials"})

    username: str | None = None
    credentials: SessionCredentials | None = None

    def as_id(self) -> PostgresGatewayUserId:
        if self.username is None:
            raise ValueError("A postgres gateway username is assigned when the user is created")
        return PostgresGatewayUserId(username=self.username)


class PostgresGatewayUserResponse(ResponseResource[PostgresGatewayUserRequest]):
    username: str
    created_time: int
    last_updated_time: int | None = None
    session_id: int | None = None

    @classmethod
    def request_cls(cls) -> type[PostgresGatewayUserRequest]:
        return PostgresGatewayUserRequest

    def as_id(self) -> PostgresGatewayUserId:
        return PostgresGatewayUserId(username=self.username)


class PostgresGatewayUserCreatedResponse(PostgresGatewayUserResponse):
    """Returned by create. The password is only returned once."""

    host: str
    password: str
