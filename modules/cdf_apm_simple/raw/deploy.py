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


class Deploy(BaseModel):
    client: CogniteClient
    content: List[RawDatabaseContent]
    reset: Optional[bool] = False

    @classmethod
    def createtables(cls):
        for table in cls.content:

            if cls.reset:
                with suppress(CogniteNotFoundError):
                    cls.client.raw.databases.delete_table(table.database_name, table.table_name)
                    
            cls.client.raw.rows.insert_dataframe(table.database_name, table.table_name, table.data, ensure_parent=True)
            Logger.info(f"Table {table.table_name} populated in database {table.database_name}")