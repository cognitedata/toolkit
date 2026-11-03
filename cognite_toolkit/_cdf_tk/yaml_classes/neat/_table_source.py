from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from functools import cache
from pathlib import Path

from cognite_toolkit._cdf_tk.utils.file import YamlPosition, safe_read, yaml_positions


@dataclass
class SpreadsheetReadContext:
    """This class is used to store information about the source spreadsheet.

    It is used to adjust row numbers to account for header rows and empty rows
    such that the error/warning messages are accurate.
    """

    header_row: int = 0
    empty_rows: list[int] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.empty_rows.sort()

    def adjusted_row_number(self, row_no: int) -> int:
        """Adjusts the given row number to account for header rows and empty rows.

        Args:
            row_no (int): The original row number (0-based).

        !!! note "Row Numbering"
            Input rows are zero-indexed, while output rows are one-indexed as they appear in Excel.
            Therefore, we are adding 1 to offset header and 1 to offset row no

        """
        output = (row_no + 1) + (self.header_row + 1)
        counter = 0
        for empty_row in self.empty_rows:
            if empty_row < self.header_row:
                continue
            if empty_row <= output:
                counter += 1
                output += 1
            else:
                break

        return output


@dataclass
class NeatTableSource(ABC):
    """Describes where a NEAT table came from, and lets the reader build human-readable locations.

    When the NEAT table comes from a YAML file, ``table_read`` has no entry for the table, so
    locations point at the table's position in the YAML structure (the 1-based list index). When the
    NEAT table comes from a spreadsheet, ``table_read`` holds a :class:`SpreadsheetReadContext` per
    sheet, so locations are adjusted to the actual Excel row numbers (accounting for the header row and
    any empty rows).
    """

    source: Path

    @abstractmethod
    def location(self, path: tuple[int | str, ...]) -> YamlPosition | str | None:
        """Builds a human-readable location string for the given path.

        Args:
            path: A location, typically originating from Pydantic validation errors.

        Returns:
            Either the prefix or the YamlPosition of the location, depending on the source of the Neat table
        """
        raise NotImplementedError()


@dataclass
class NeatSpreadsheetSource(NeatTableSource):
    table_read: dict[str, SpreadsheetReadContext]

    def location(self, path: tuple[int | str, ...]) -> str:
        """Builds a human-readable location string for the given path.

        Args:
            path: A location, typically ``(table,)``, ``(table, row)``, or ``(table, row, column)``.
                Any further entries are joined onto the end as an attribute path.
        """
        table_id: str | None = None
        row_no: int | None = None
        column: str | None = None
        if len(path) >= 1 and isinstance(path[0], str):
            table_id = path[0]
        if len(path) >= 2 and isinstance(path[1], int):
            row_no = path[1]
        if len(path) >= 3 and isinstance(path[2], str):
            column = path[2]
        if isinstance(row_no, int):
            row_no = self.adjust_row_number(table_id, row_no)

        location_parts: list[str] = []
        if table_id is not None:
            location_parts.append(f"table {table_id!r}")
        if row_no is not None:
            location_parts.append(f"row {row_no}")
        if column is not None:
            location_parts.append(f"column {column!r}")
        if len(path) >= 4:
            location_parts.append("-> " + ".".join(str(part) for part in path[3:]))
        return " ".join(location_parts)

    def adjust_row_number(self, table_id: str | None, row_no: int) -> int:
        """Converts a 0-based row index into a 1-based row number.

        If the table was read from a spreadsheet, the row number is adjusted to account for the
        header row and any empty rows, so it matches the row as it appears in Excel. Otherwise
        (for example, for a YAML file), the row number is simply the 1-based list index.
        """
        table_read = self.table_read.get(table_id) if table_id else None
        if table_read is not None:
            return table_read.adjusted_row_number(row_no)
        return row_no + 1


class NeatYAMLSource(NeatTableSource):
    @cache
    def _yaml_positions(self) -> dict[tuple[int | str, ...], YamlPosition]:
        return yaml_positions(safe_read(self.source))

    def location(self, path: tuple[int | str, ...]) -> YamlPosition | None:
        """Builds a human-readable location string for the given path.

        Args:
            path: A location, typically originating from Pydantic validation errors.

        Returns:
            Either the prefix or the YamlPosition of the location, depending on the source of the Neat table
        """
        return self._yaml_positions().get(path)
