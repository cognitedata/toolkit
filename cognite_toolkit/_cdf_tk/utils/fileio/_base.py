from abc import ABC
from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any, ClassVar, Protocol, TypeAlias, TypeVar

from cognite_toolkit._cdf_tk.utils.useful_types import DataType, JsonVal

PrimaryCellValue: TypeAlias = datetime | date | JsonVal
CellValue: TypeAlias = PrimaryCellValue | list[PrimaryCellValue]
Chunk: TypeAlias = dict[str, CellValue]

SQLITE_METADATA_TABLE = "_toolkit_tables"


class _Closable(Protocol):
    def close(self) -> None: ...


T_IO = TypeVar("T_IO", bound=_Closable)


class FileIO(ABC):
    FORMAT: ClassVar[str]

    @property
    def format(self) -> str:
        return self.FORMAT


@dataclass(frozen=True)
class SchemaColumn:
    name: str
    type: DataType
    is_array: bool = False

    def __post_init__(self) -> None:
        if self.type == "json" and self.is_array:
            raise ValueError("JSON columns cannot be arrays. Use 'is_array=False' for JSON columns.")

    def dump(self) -> dict[str, JsonVal]:
        return asdict(self)

    @classmethod
    def load(cls, data: dict[str, Any]) -> "SchemaColumn":
        return cls(**data)


@dataclass
class SQLiteTable:
    """A data table in a SQLite database written by Toolkit.

    Args:
        name: The name of the table.
        schema: The schema of the table, used to restore the original data types when reading.
        metadata: Optional metadata stored together with the table.
    """

    name: str
    schema: list[SchemaColumn]
    metadata: dict[str, JsonVal] | None = None


def quote_sqlite_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'
