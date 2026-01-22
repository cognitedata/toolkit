import sys

from pydantic import Field

from cognite_toolkit._cdf_tk.client._resource_base import (
    Identifier,
    RequestResource,
    ResponseResource,
)

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class RAWDatabase(RequestResource, Identifier, ResponseResource["RAWDatabase"]):
    name: str

    def as_id(self) -> Self:
        return self

    def __str__(self) -> str:
        return f"name='{self.name}'"

    def as_request_resource(self) -> "RAWDatabase":
        return type(self).model_validate(self.dump(), extra="ignore")


class RAWTable(RequestResource, Identifier, ResponseResource["RAWTable"]):
    # This is a query parameter, so we exclude it from serialization.
    # Default to empty string to allow parsing from API responses (which don't include db_name).
    db_name: str = Field(default="", exclude=True)
    name: str

    def as_id(self) -> Self:
        return self

    def __str__(self) -> str:
        return f"dbName='{self.db_name}', tableName='{self.name}'"

    def as_request_resource(self) -> "RAWTable":
        dumped = {**self.dump(), "dbName": self.db_name}
        return type(self).model_validate(dumped, extra="ignore")
