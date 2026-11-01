from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from openpyxl import load_workbook

from ._yaml_class import NeatYAML


@dataclass
class SpreadsheetReadContext:
    """Information needed to report table row numbers as Excel row numbers."""

    header_row: int = 0
    empty_rows: list[int] = field(default_factory=list)

    def adjusted_row_number(self, row_no: int) -> int:
        output = row_no + self.header_row + 2
        for empty_row in self.empty_rows:
            if empty_row >= self.header_row and empty_row <= output:
                output += 1
        return output


@dataclass
class SpreadsheetWarning:
    message: str
    table: str | None = None
    row: int | None = None

    def display(self, contexts: dict[str, SpreadsheetReadContext]) -> str:
        if self.table is None:
            return self.message
        location = f"Spreadsheet table {self.table!r}"
        if self.row is not None:
            location += f" row {contexts[self.table].adjusted_row_number(self.row)}"
        return f"{location}: {self.message}"


_REQUIRED_COLUMNS = {
    "Metadata": ["Key", "Value"],
    "Properties": ["View", "View Property", "Connection", "Value Type", "Min Count", "Max Count"],
    "Views": ["View"],
    "Containers": ["Container"],
    "Enum": ["Collection", "Value"],
    "Nodes": ["Node"],
}
_REQUIRED_TABLES = frozenset({"Metadata", "Properties", "Views"})


def read_neat_spreadsheet(path: Path) -> tuple[NeatYAML, list[SpreadsheetWarning]]:
    """Read a NEAT workbook into the same validated model used for table YAML."""
    try:
        workbook = load_workbook(path, read_only=True, data_only=True, rich_text=False)
    except (OSError, ValueError, KeyError) as error:
        raise ValueError(f"Could not open spreadsheet {path.name!r}: {error}") from error

    tables: dict[str, list[dict[str, Any]]] = {}
    warnings: list[SpreadsheetWarning] = []
    contexts: dict[str, SpreadsheetReadContext] = {}
    try:
        missing = _REQUIRED_TABLES.difference(workbook.sheetnames)
        if missing:
            sheets = ", ".join(repr(name) for name in sorted(missing))
            raise ValueError(f"Missing required spreadsheet sheet(s): {sheets}.")

        for table_name, required_columns in _REQUIRED_COLUMNS.items():
            if table_name not in workbook.sheetnames:
                continue
            rows, context, table_warnings = _read_sheet(workbook[table_name], table_name, required_columns)
            tables[table_name] = rows
            contexts[table_name] = context
            warnings.extend(table_warnings)
    finally:
        workbook.close()

    try:
        return NeatYAML.model_validate(tables), [SpreadsheetWarning(warning.display(contexts)) for warning in warnings]
    except ValueError as error:
        raise ValueError(_format_validation_error(error, contexts)) from error


def _read_sheet(
    sheet: Any, table_name: str, required_columns: list[str]
) -> tuple[list[dict[str, Any]], SpreadsheetReadContext, list[SpreadsheetWarning]]:
    context = SpreadsheetReadContext()
    rows: list[dict[str, Any]] = []
    warnings: list[SpreadsheetWarning] = []
    headers: list[str] | None = required_columns if table_name == "Metadata" else None

    for row_no, row in enumerate(sheet.iter_rows(values_only=True)):
        if headers is None:
            values = [str(value).strip() if value is not None else "" for value in row]
            if set(values).intersection(required_columns):
                headers = values
                context.header_row = row_no
            continue
        if all(value is None for value in row):
            context.empty_rows.append(row_no)
            continue
        values = dict(zip(headers, row, strict=False))
        if all(value is None for value in values.values()):
            continue
        rows.append(values)

    if headers is None:
        raise ValueError(f"Spreadsheet table {table_name!r} is missing its column header row.")
    missing = [column for column in required_columns if column not in headers]
    if missing:
        columns = ", ".join(repr(column) for column in missing)
        raise ValueError(f"Spreadsheet table {table_name!r} is missing required column(s): {columns}.")
    if not rows:
        warnings.append(SpreadsheetWarning("contains no data rows.", table_name))
    return rows, context, warnings


def _format_validation_error(error: ValueError, contexts: dict[str, SpreadsheetReadContext]) -> str:
    """Add sheet and row context to Pydantic's concise validation errors."""
    errors = getattr(error, "errors", None)
    if not callable(errors):
        return str(error)
    formatted: list[str] = []
    for detail in errors(include_url=False):
        location = detail["loc"]
        table = location[0] if location and isinstance(location[0], str) else None
        row = location[1] if len(location) > 1 and isinstance(location[1], int) else None
        prefix = ""
        if table is not None:
            prefix = f"Spreadsheet table {table!r}"
            if row is not None and table in contexts:
                prefix += f" row {contexts[table].adjusted_row_number(row)}"
            prefix += ": "
        formatted.append(f"{prefix}{detail['msg']}")
    return "\n".join(formatted) or str(error)
