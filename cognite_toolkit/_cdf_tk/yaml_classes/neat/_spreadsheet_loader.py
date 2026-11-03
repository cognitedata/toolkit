from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, TypeAlias

from ._table_source import NeatTableSource

if TYPE_CHECKING:
    pass

# These are the types that openpyxl supports in cells
CellValueType: TypeAlias = str | int | float | bool | datetime | date | time | timedelta | None

# The format expected for excel sheets representing a data model
DataModelTableType: TypeAlias = dict[str, list[dict[str, CellValueType]]]


def _load_spreadsheet(spreadsheet: Path) -> tuple[DataModelTableType, NeatTableSource]:
    raise NotImplementedError()
