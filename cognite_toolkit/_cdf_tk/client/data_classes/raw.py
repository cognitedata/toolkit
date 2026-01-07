from cognite_toolkit._cdf_tk.client.data_classes.base import (
    Identifier,
    RequestResource,
    ResponseResource,
)


class DatabaseName(Identifier):
    db_name: str

    def __str__(self) -> str:
        return f"dbName='{self.db_name}'"


class TableName(Identifier):
    db_name: str
    table_name: str

    def __str__(self) -> str:
        return f"dbName='{self.db_name}', tableName='{self.table_name}'"


class DatabaseRequest(RequestResource):
    db_name: str

    def as_id(self) -> DatabaseName:
        return DatabaseName(db_name=self.db_name)


class DatabaseResponse(ResponseResource[DatabaseRequest]):
    db_name: str
    created_time: int | None = None

    def as_request_resource(self) -> DatabaseRequest:
        return DatabaseRequest.model_validate(self.dump(), extra="ignore")


class TableRequest(RequestResource):
    db_name: str
    table_name: str

    def as_id(self) -> TableName:
        return TableName(db_name=self.db_name, table_name=self.table_name)


class TableResponse(ResponseResource[TableRequest]):
    db_name: str
    table_name: str
    created_time: int | None = None

    def as_request_resource(self) -> TableRequest:
        return TableRequest.model_validate(self.dump(), extra="ignore")

