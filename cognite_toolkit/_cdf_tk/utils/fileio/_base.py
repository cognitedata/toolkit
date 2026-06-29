from abc import ABC
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, ClassVar, Protocol, TypeAlias, TypeVar

from cognite_toolkit._cdf_tk.utils.useful_types import DataType, JsonVal

PrimaryCellValue: TypeAlias = datetime | date | JsonVal
CellValue: TypeAlias = PrimaryCellValue | list[PrimaryCellValue]
Chunk: TypeAlias = dict[str, CellValue]


class SupportsClose(Protocol):
    def close(self) -> Any: ...


T_IO = TypeVar("T_IO", bound=SupportsClose)


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
