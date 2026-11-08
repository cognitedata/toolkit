from typing import Literal, Self, TypeAlias

from pydantic import Field, model_validator

from cognite_toolkit._cdf_tk.client.identifiers import PostgresGatewayTableId

from .base import BaseModelResource, ToolkitResource

PostgresGatewayColumnType: TypeAlias = Literal[
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


class PostgresGatewayColumnYAML(BaseModelResource):
    type: PostgresGatewayColumnType = Field(description="The Postgres type of the column.")


class RawTableOptionsYAML(BaseModelResource):
    database: str = Field(description="The name of the RAW database.")
    table: str = Field(description="The name of the RAW table.")
    primary_key: str | None = Field(default=None, description="The column that holds the key of the RAW rows.")


class ViewTableOptionsYAML(BaseModelResource):
    space: str = Field(description="The space of the view.")
    external_id: str = Field(description="The external ID of the view.")
    version: str | None = Field(default=None, description="The version of the view.")


class PostgresGatewayTableYAML(ToolkitResource):
    """A foreign table exposed through the Postgres gateway, backed by a RAW table or a data modeling view."""

    username: str = Field(description="The username of the Postgres gateway user that owns the table.", min_length=1)
    tablename: str = Field(
        description="The name of the foreign table. Cannot be the name of a built-in table.",
        min_length=1,
        max_length=60,
    )
    type: Literal["raw_rows", "view"] = Field(
        description="Whether the table exposes the rows of a RAW table or the instances of a view."
    )
    options: RawTableOptionsYAML | ViewTableOptionsYAML = Field(
        description="The RAW table for raw_rows tables, or the view for view tables."
    )
    columns: dict[str, PostgresGatewayColumnYAML] | None = Field(
        default=None,
        description="The columns of a raw_rows table by column name. View tables get their columns from the view.",
    )

    @model_validator(mode="after")
    def check_options_match_type(self) -> Self:
        if self.type == "raw_rows":
            if not isinstance(self.options, RawTableOptionsYAML):
                raise ValueError("A raw_rows table requires options with database and table.")
            if not self.columns:
                raise ValueError("A raw_rows table requires columns.")
            return self
        if not isinstance(self.options, ViewTableOptionsYAML):
            raise ValueError("A view table requires options with space and externalId.")
        if self.columns is not None:
            raise ValueError("A view table gets its columns from the view, so columns cannot be set.")
        return self

    def as_id(self) -> PostgresGatewayTableId:
        return PostgresGatewayTableId(username=self.username, tablename=self.tablename)
