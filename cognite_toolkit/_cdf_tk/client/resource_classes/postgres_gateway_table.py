"""Resource classes for the Postgres Gateway Tables API.

Based on https://api-docs.cognite.com/20230101/tag/Postgres-Gateway-Tables
"""

from pydantic import Field, JsonValue

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, RequestResource, ResponseResource
from cognite_toolkit._cdf_tk.client.identifiers import PostgresGatewayTableId


class PostgresGatewayColumn(BaseModelObject):
    """A column of a foreign table, for example ``TEXT``, ``BIGINT``, or ``JSON[]``."""

    type: str


class RawTableOptions(BaseModelObject):
    """Exposes the rows of a RAW table."""

    database: str
    table: str
    primary_key: str | None = None


class ViewTableOptions(BaseModelObject):
    """Exposes the instances of a data modeling view."""

    space: str
    external_id: str
    version: str | None = None


class PostgresGatewayTableRequest(RequestResource):
    """Request resource for creating postgres gateway tables. Tables cannot be updated."""

    # This is a path parameter, so we exclude it from serialization.
    username: str = Field(exclude=True)
    tablename: str
    type: str
    options: RawTableOptions | ViewTableOptions
    columns: dict[str, PostgresGatewayColumn] | None = None

    def as_id(self) -> PostgresGatewayTableId:
        return PostgresGatewayTableId(username=self.username, tablename=self.tablename)


class PostgresGatewayTableResponse(ResponseResource[PostgresGatewayTableRequest]):
    """A postgres gateway table.

    Tables created from a view are returned with ``type`` ``nodes`` or ``edges``, the columns of the view, and
    empty ``options``, as the API does not return the view. Built-in tables have ``type`` ``built_in`` and no
    ``options``. Only create returns ``created_time``.
    """

    # This is a path parameter, so we exclude it from serialization.
    # Default to empty string to allow parsing from API responses (which don't include username).
    username: str = Field(default="", exclude=True)
    tablename: str
    type: str
    columns: dict[str, PostgresGatewayColumn]
    options: RawTableOptions | ViewTableOptions | dict[str, JsonValue] | None = None
    created_time: int | None = None

    @classmethod
    def request_cls(cls) -> type[PostgresGatewayTableRequest]:
        return PostgresGatewayTableRequest

    def as_request_resource(self) -> PostgresGatewayTableRequest:
        return PostgresGatewayTableRequest.model_validate(
            {**self.dump(), "username": self.username}, extra="ignore", by_alias=True
        )

    def as_id(self) -> PostgresGatewayTableId:
        return PostgresGatewayTableId(username=self.username, tablename=self.tablename)
