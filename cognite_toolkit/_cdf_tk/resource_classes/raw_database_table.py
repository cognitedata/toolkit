from pydantic import Field

from cognite_toolkit._cdf_tk.client.identifiers import RawDatabaseId, RawTableId

from .base import ToolkitResource


class DatabaseYAML(ToolkitResource):
    db_name: str = Field(
        description="The name of the database.",
        min_length=1,
        max_length=32,
    )

    def as_id(self) -> RawDatabaseId:
        return RawDatabaseId(name=self.db_name)


class TableYAML(ToolkitResource, populate_by_name=True):
    db_name: str = Field(
        description="The name of the database.",
        min_length=1,
        max_length=32,
    )
    table_name: str = Field(
        description="The name of the table.",
        min_length=1,
        max_length=64,
    )

    def as_id(self) -> RawTableId:
        return RawTableId(db_name=self.db_name, name=self.table_name)
