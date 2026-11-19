"""Resource classes for the Postgres Gateway Tables API.

Based on https://api-docs.cognite.com/20230101/tag/Postgres-Gateway-Tables
"""

import builtins
from typing import Annotated, Any, Literal

from pydantic import ConfigDict, Discriminator, Field, JsonValue, Tag, TypeAdapter

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, RequestResource, ResponseResource
from cognite_toolkit._cdf_tk.client.identifiers import PostgresGatewayTableId

PostgresGatewayTableColumnType = Literal[
    "TEXT",
    "VARCHAR",
    "BOOL",
    "BIGINT",
    "DOUBLE PRECISION",
    "REAL",
    "TIMESTAMPTZ",
    "JSON",
    "TEXT[]",
    "VARCHAR[]",
    "BOOL[]",
    "BIGINT[]",
    "DOUBLE PRECISION[]",
    "REAL[]",
    "JSON[]",
    "BYTEA",
]


class PostgresGatewayColumn(BaseModelObject):
    """A column of a foreign table, for example ``TEXT``, ``BIGINT``, or ``JSON[]``."""

    type: PostgresGatewayTableColumnType | str


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


class RawRowsTable(BaseModelObject):
    """The fields of a table exposing RAW rows, shared by the request and the response."""

    type: Literal["raw_rows"] = "raw_rows"
    options: RawTableOptions
    columns: dict[str, PostgresGatewayColumn]


class PostgresGatewayTableRequest(RequestResource):
    """Base request resource for creating postgres gateway tables. Tables cannot be updated."""

    # This is a path parameter, so we exclude it from serialization.
    username: str = Field(exclude=True)
    tablename: str
    type: str

    def as_id(self) -> PostgresGatewayTableId:
        return PostgresGatewayTableId(username=self.username, tablename=self.tablename)


class RawRowsTableRequest(RawRowsTable, PostgresGatewayTableRequest):
    """Creates a table exposing the rows of a RAW table."""


class ViewTableRequest(PostgresGatewayTableRequest):
    """Creates a table exposing the instances of a view. The columns are derived from the view."""

    type: Literal["view"] = "view"
    options: ViewTableOptions


class UnknownTableRequest(PostgresGatewayTableRequest):
    """A table of a type not known to the Toolkit. Any other fields are kept as extra fields."""

    type: str


class PostgresGatewayTableResponse(ResponseResource[PostgresGatewayTableRequest]):
    """Base response resource for postgres gateway tables. Only create returns ``created_time``."""

    # This is a path parameter, so we exclude it from serialization.
    # Default to empty string to allow parsing from API responses (which don't include username).
    username: str = Field(default="", exclude=True)
    tablename: str
    type: str
    created_time: int | None = None

    @classmethod
    def request_cls(cls) -> builtins.type[PostgresGatewayTableRequest]:
        return PostgresGatewayTableRequest

    def as_request_resource(self) -> PostgresGatewayTableRequest:
        return self.request_cls().model_validate(
            {**self.dump(), "username": self.username}, extra="ignore", by_alias=True
        )

    def as_id(self) -> PostgresGatewayTableId:
        return PostgresGatewayTableId(username=self.username, tablename=self.tablename)


class RawRowsTableResponse(RawRowsTable, PostgresGatewayTableResponse):
    """A table exposing the rows of a RAW table."""

    @classmethod
    def request_cls(cls) -> builtins.type[RawRowsTableRequest]:
        return RawRowsTableRequest

    def as_request_resource(self) -> RawRowsTableRequest:
        return RawRowsTableRequest.model_validate(
            {**self.dump(), "username": self.username}, extra="ignore", by_alias=True
        )


class ViewTableResponse(PostgresGatewayTableResponse):
    """A table created from a view.

    The API returns the type ``nodes`` or ``edges``, the columns of the view, and empty ``options``, as it
    does not return the view.
    """

    type: Literal["nodes", "edges"]
    columns: dict[str, PostgresGatewayColumn]
    options: dict[str, JsonValue] | None = None

    @classmethod
    def request_cls(cls) -> builtins.type[ViewTableRequest]:
        return ViewTableRequest

    def as_request_resource(self) -> ViewTableRequest:
        raise ValueError(
            f"Cannot convert table {self.tablename!r} to a request, as the API does not return the view it was created from."
        )


class BuiltInTableResponse(PostgresGatewayTableResponse):
    """A built-in table. Built-in tables cannot be created."""

    type: Literal["built_in"]
    columns: dict[str, PostgresGatewayColumn]
    options: dict[str, JsonValue] | None = None

    def as_request_resource(self) -> PostgresGatewayTableRequest:
        raise ValueError(f"Cannot convert built-in table {self.tablename!r} to a request.")


class UnknownTableResponse(PostgresGatewayTableResponse):
    """A table of a type not known to the Toolkit."""

    columns: dict[str, PostgresGatewayColumn] | None = None
    options: dict[str, JsonValue] | None = None

    @classmethod
    def request_cls(cls) -> builtins.type[UnknownTableRequest]:
        return UnknownTableRequest

    def as_request_resource(self) -> UnknownTableRequest:
        return UnknownTableRequest.model_validate(
            {**self.dump(), "username": self.username}, extra="allow", by_alias=True
        )


_RESPONSE_TAG_BY_TYPE = {"raw_rows": "raw_rows", "nodes": "view", "edges": "view", "built_in": "built_in"}


def _response_tag(value: Any) -> str:
    table_type = value.get("type") if isinstance(value, dict) else getattr(value, "type", None)
    return _RESPONSE_TAG_BY_TYPE.get(table_type, "unknown") if isinstance(table_type, str) else "unknown"


PostgresGatewayTableResponseUnion = Annotated[
    Annotated[RawRowsTableResponse, Tag("raw_rows")]
    | Annotated[ViewTableResponse, Tag("view")]
    | Annotated[BuiltInTableResponse, Tag("built_in")]
    | Annotated[UnknownTableResponse, Tag("unknown")],
    Discriminator(_response_tag),
]

PostgresGatewayTableResponseAdapter: TypeAdapter[PostgresGatewayTableResponseUnion] = TypeAdapter(
    PostgresGatewayTableResponseUnion, config=ConfigDict(defer_build=True)
)
