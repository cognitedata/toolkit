from cognite_toolkit._cdf_tk.client.http_client import HTTPClient

from .postgres_gateway_users import PostgresGatewayUsersAPI


class PostgresGatewayAPI:
    """API for Postgres Gateway resources.

    This class groups all Postgres Gateway related APIs:
    - users: Manage the postgres users that own foreign tables
    """

    def __init__(self, http_client: HTTPClient) -> None:
        self.users = PostgresGatewayUsersAPI(http_client)
