from pydantic import Field

from .base import ToolkitResource


class DatabaseYAML(ToolkitResource):
    db_name: str | list[str] = Field(description="The name or list of names of the database.")


class TableYAML(ToolkitResource):
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
