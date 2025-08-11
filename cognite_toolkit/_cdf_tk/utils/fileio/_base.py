from abc import ABC
from datetime import date, datetime
from typing import IO, ClassVar, Literal, TypeAlias, TypeVar

from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

DataType: TypeAlias = Literal["string", "integer", "float", "boolean", "json", "date", "timestamp"]
PrimaryCellValue: TypeAlias = datetime | date | JsonVal
CellValue: TypeAlias = PrimaryCellValue | list[PrimaryCellValue]
Chunk: TypeAlias = dict[str, CellValue]


T_IO = TypeVar("T_IO", bound=IO)


class FileIO(ABC):
    format: ClassVar[str]
