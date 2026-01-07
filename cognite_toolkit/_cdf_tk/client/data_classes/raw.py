from cognite_toolkit._cdf_tk.client.data_classes.base import (
    Identifier,
    RequestResource,
    ResponseResource,
)
import sys
if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

class DatabaseRequest(RequestResource, Identifier):
    db_name: str

    def as_id(self) -> Self:
        return self

    def __str__(self) -> str:
        return f"dbName='{self.db_name}'"


class DatabaseResponse(ResponseResource[DatabaseRequest]):
    db_name: str
    created_time: int | None = None

    def as_request_resource(self) -> DatabaseRequest:
        return DatabaseRequest.model_validate(self.dump(), extra="ignore")


class TableRequest(RequestResource, Identifier):
    db_name: str
    table_name: str

    def as_id(self) -> Self:
        return

    def __str__(self) -> str:
        return f"dbName='{self.db_name}', tableName='{self.table_name}'"


class TableResponse(ResponseResource[TableRequest]):
    db_name: str
    table_name: str
    created_time: int | None = None

    def as_request_resource(self) -> TableRequest:
        return TableRequest.model_validate(self.dump(), extra="ignore")

