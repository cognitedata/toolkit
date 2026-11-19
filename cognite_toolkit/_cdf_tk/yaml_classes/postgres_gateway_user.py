from pydantic import Field

from cognite_toolkit._cdf_tk.client.identifiers import PostgresGatewayUserId

from .authentication import AuthenticationClientIdSecret
from .base import ToolkitResource


class PostgresGatewayUserYAML(ToolkitResource):
    """An existing Postgres gateway user. CDF generates the username when the user is created."""

    username: str = Field(
        description="The username of the Postgres gateway user. The user must already exist in CDF.",
        min_length=1,
    )
    credentials: AuthenticationClientIdSecret | None = Field(
        default=None,
        description="Client credentials used to create the CDF session the Postgres gateway user acts with. "
        "If not set, Toolkit does not change the session of the user.",
    )

    def as_id(self) -> PostgresGatewayUserId:
        return PostgresGatewayUserId(username=self.username)
