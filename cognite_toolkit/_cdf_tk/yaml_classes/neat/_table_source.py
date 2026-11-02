from dataclasses import dataclass, field


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
class NeatTableSource:
    source: str
    table_read: dict[str, SpreadsheetReadContext] = field(default_factory=dict)
