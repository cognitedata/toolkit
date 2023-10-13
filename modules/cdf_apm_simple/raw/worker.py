from contextlib import suppress
from logging import Logger
from typing import List, Optional

from pandas import DataFrame
from pydantic import BaseModel
from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteNotFoundError


class RawDatabaseContent(BaseModel):
    database_name: str
    table_name: str
    data: DataFrame
    class Config:
        arbitrary_types_allowed = True


class Worker(BaseModel):
    client: CogniteClient
    content: List[RawDatabaseContent]
    reset: Optional[bool] = False

    class Config:
        arbitrary_types_allowed = True

    #@classmethod
    def createtables(self):
        for table in self.content:

            if self.reset:
                with suppress(CogniteNotFoundError):
                    self.client.raw.databases.delete_table(table.database_name, table.table_name)
                    
            self.client.raw.rows.insert_dataframe(table.database_name, table.table_name, table.data, ensure_parent=True)
            # todo: add logging
            #Logger.info(self, msg=f"Table {table.table_name} populated in database {table.database_name}")