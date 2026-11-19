from typing import Annotated, Literal

from pydantic import Field

from cognite_toolkit._cdf_tk.client.identifiers import PostgresGatewayTableId
from cognite_toolkit._cdf_tk.client.resource_classes.postgres_gateway_table import PostgresGatewayTableColumnType

from .base import BaseModelResource, ToolkitResource


class PostgresGatewayColumnYAML(BaseModelResource):
    type: PostgresGatewayTableColumnType = Field(description="The Postgres type of the column.")


class RawTableOptionsYAML(BaseModelResource):
    database: str = Field(description="The name of the RAW database.")
    table: str = Field(description="The name of the RAW table.")
    primary_key: str | None = Field(default=None, description="The column that holds the key of the RAW rows.")


class ViewTableOptionsYAML(BaseModelResource):
    space: str = Field(description="The space of the view.")
    external_id: str = Field(description="The external ID of the view.")
    version: str | None = Field(default=None, description="The version of the view.")


class PostgresGatewayTable(ToolkitResource):
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

    def as_id(self) -> PostgresGatewayTableId:
        return PostgresGatewayTableId(username=self.username, tablename=self.tablename)


class RawRowsTableYAML(PostgresGatewayTable):
    """A table exposing the rows of a RAW table."""

    type: Literal["raw_rows"] = Field("raw_rows", description="The table exposes the rows of a RAW table.")
    options: RawTableOptionsYAML = Field(description="The RAW table to expose.")
    columns: dict[str, PostgresGatewayColumnYAML] = Field(
        description="The columns of the table by column name.", min_length=1
    )


class ViewTableYAML(PostgresGatewayTable):
    """A table exposing the instances of a view. The columns are derived from the view."""

    type: Literal["view"] = Field("view", description="The table exposes the instances of a view.")
    options: ViewTableOptionsYAML = Field(description="The view to expose.")


PostgresGatewayTableYAML = Annotated[RawRowsTableYAML | ViewTableYAML, Field(discriminator="type")]
