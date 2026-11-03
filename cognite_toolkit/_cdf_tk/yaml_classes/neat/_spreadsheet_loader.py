import importlib.util
import warnings
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, TypeAlias, cast

from cognite_toolkit._cdf_tk.exceptions import ToolkitMissingDependencyError

from ._constant import METADATA_SHEET
from ._table_source import NeatSpreadsheetSource, NeatTableSource, SpreadsheetReadContext

if TYPE_CHECKING:
    from openpyxl.worksheet.worksheet import Worksheet

# These are the types that openpyxl supports in cells
CellValueType: TypeAlias = str | int | float | bool | datetime | date | time | timedelta | None

# The format expected for excel sheets representing a data model
DataModelTableType: TypeAlias = dict[str, list[dict[str, CellValueType]]]


def _load_spreadsheet(spreadsheet: Path) -> tuple[DataModelTableType, NeatTableSource]:
    if importlib.util.find_spec("openpyxl") is None:
        raise ToolkitMissingDependencyError(
            "Reading a spreadsheet requires openpyxl. Install with 'pip install \"cognite-toolkit[neat]\"'"
        )
    from openpyxl import load_workbook

    # Avoid circular import.
    from cognite_toolkit._cdf_tk.yaml_classes.neat._neat_format import NeatYAML

    tables: DataModelTableType = {}
    source = NeatSpreadsheetSource(source=spreadsheet)
    workbook = load_workbook(spreadsheet, read_only=True, data_only=True, rich_text=False)
    try:
        for column_id, column_info in NeatYAML.model_fields.items():
            sheet_name = cast(str, column_info.validation_alias)
            if sheet_name not in workbook.sheetnames:
                continue
            if sheet_name == METADATA_SHEET:
                required_columns = ["Key", "Value"]
            else:
                required_columns = NeatYAML.get_sheet_columns(column_id, column_info, column_type="required")
            sheet = workbook[sheet_name]
            context = SpreadsheetReadContext()
            table_rows = _read_rows(sheet, required_columns, context)
            tables[sheet_name] = table_rows
            source.table_read[sheet_name] = context
        return tables, source
    finally:
        workbook.close()


def _read_rows(
    sheet: "Worksheet", required_columns: list[str], context: SpreadsheetReadContext
) -> list[dict[str, CellValueType]]:
    table_rows: list[dict[str, CellValueType]] = []
    # Metadata sheet is just a key-value pair of the first two columns.
    # For other sheets, we need to find the column header row first.
    columns: list[str] = [] if sheet.title != METADATA_SHEET else required_columns
    # The .iter_rows with values_only=True raises the warning `UserWarning: Data Validation extension is
    # not supported and will be removed`
    # which confuses the user and we do not depend on data validation here, so we suppress it.
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning, message="Data Validation extension is not supported*")
        for row_no, row in enumerate(sheet.iter_rows(values_only=True)):
            if columns:
                # We have found the column header row, read the data rows.
                if all(cell is None for cell in row):
                    context.empty_rows.append(row_no)
                else:
                    record = dict(zip(columns, row, strict=False))
                    table_rows.append(record)
            else:
                # Look for the column header row.
                row_values = [str(cell) for cell in row]
                if set(row_values).intersection(required_columns):
                    columns = row_values
                    context.header_row = row_no

    return table_rows
