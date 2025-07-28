import csv
import gzip
import importlib.util
import json
import sys
from abc import ABC, abstractmethod
from collections import Counter
from collections.abc import Iterable, Iterator, Mapping
from datetime import date, datetime, timezone
from functools import lru_cache
from io import IOBase, TextIOWrapper
from pathlib import Path
from typing import IO, Any, ClassVar, Literal, TypeAlias, TypeVar, Generic

from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError, ToolkitMissingDependencyError, ToolkitTypeError
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


PrimaryCellValue: TypeAlias = datetime | date | JsonVal
CellValue: TypeAlias = PrimaryCellValue | list[PrimaryCellValue]
Chunk: TypeAlias = dict[str, CellValue]


T_IO = TypeVar("T_IO", bound=IO)


class FileIO(ABC):
    format: ClassVar[str]


