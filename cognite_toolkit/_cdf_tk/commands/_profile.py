from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import cached_property, partial
from typing import ClassVar, Literal, TypeAlias, overload

from cognite.client.data_classes import Transformation
from cognite.client.exceptions import CogniteException
from rich import box
from rich.console import Console
from rich.live import Live
from rich.spinner import Spinner
from rich.table import Table

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.raw import RawProfileResults, RawTable
from cognite_toolkit._cdf_tk.utils.aggregators import (
    AssetAggregator,
    AssetCentricAggregator,
    EventAggregator,
    FileAggregator,
    LabelAggregator,
    LabelCountAggregator,
    MetadataAggregator,
    RelationshipAggregator,
    SequenceAggregator,
    TimeSeriesAggregator,
)
from cognite_toolkit._cdf_tk.utils.cdf import get_transformation_sources

from ._base import ToolkitCommand


class WaitingAPICallClass:
    def __bool__(self) -> bool:
        return False


WaitingAPICall = WaitingAPICallClass()

PendingCellValue: TypeAlias = int | float | str | bool | None | WaitingAPICallClass
CellValue: TypeAlias = int | float | str | bool | None
PendingTable: TypeAlias = dict[tuple[str, str], PendingCellValue]


class ProfileCommand(ToolkitCommand, ABC):
    def __init__(self, print_warning: bool = True, skip_tracking: bool = False, silent: bool = False) -> None:
        super().__init__(print_warning, skip_tracking, silent)
        self.table_title = self.__class__.__name__.removesuffix("Command")

    class Columns:  # Placeholder for columns, subclasses should define their own Columns class
        ...

    spinner_args: ClassVar[Mapping] = dict(name="arc", text="loading...", style="bold green", speed=1.0)

    max_workers = 8
    is_dynamic_table = False

    @cached_property
    def columns(self) -> tuple[str, ...]:
        return (
            tuple([attr for attr in self.Columns.__dict__.keys() if not attr.startswith("_")])
            if hasattr(self, "Columns")
            else tuple()
        )

    def create_profile_table(self, client: ToolkitClient) -> list[dict[str, CellValue]]:
        console = Console()
        with console.status("Setting up", spinner="aesthetic", speed=0.4) as _:
            table = self.create_initial_table(client)
        with (
            Live(self.draw_table(table), refresh_per_second=4, console=console) as live,
            ThreadPoolExecutor(max_workers=self.max_workers) as executor,
        ):
            while True:
                current_calls = {
                    executor.submit(self.call_api(row, col, client)): (row, col)
                    for (row, col), cell in table.items()
                    if cell is WaitingAPICall
                }
                if not current_calls:
                    break
                for future in as_completed(current_calls):
                    row, col = current_calls[future]
                    try:
                        result = future.result()
                    except CogniteException as e:
                        result = type(e).__name__
                    table[(row, col)] = self.format_result(result, row, col)
                    if self.is_dynamic_table:
                        table = self.update_table(table, result, row, col)
                    live.update(self.draw_table(table))
        return self.as_record_format(table, allow_waiting_api_call=False)

    @abstractmethod
    def create_initial_table(self, client: ToolkitClient) -> PendingTable:
        """
        Create the initial table with placeholders for API calls.
        Each cell that requires an API call should be initialized with WaitingAPICall.
        """
        raise NotImplementedError("Subclasses must implement create_initial_table.")

    @abstractmethod
    def call_api(self, row: str, col: str, client: ToolkitClient) -> Callable:
        raise NotImplementedError("Subclasses must implement call_api.")

    def format_result(self, result: object, row: str, col: str) -> CellValue:
        """
        Format the result of an API call for display in the table.
        This can be overridden by subclasses to customize formatting.
        """
        if isinstance(result, int | float | bool | str):
            return result
        raise NotImplementedError("Subclasses must implement format_result.")

    def update_table(
        self,
        current_table: PendingTable,
        result: object,
        row: str,
        col: str,
    ) -> PendingTable:
        raise NotImplementedError("Subclasses must implement update_table.")

    def draw_table(self, table: PendingTable) -> Table:
        rich_table = Table(
            title=self.table_title,
            title_justify="left",
            show_header=True,
            header_style="bold magenta",
            box=box.MINIMAL,
        )
        for col in self.columns:
            rich_table.add_column(col)

        rows = self.as_record_format(table)

        for row in rows:
            rich_table.add_row(*[self._as_cell(value) for value in row.values()])
        return rich_table

    @classmethod
    @overload
    def as_record_format(
        cls, table: PendingTable, allow_waiting_api_call: Literal[True] = True
    ) -> list[dict[str, PendingCellValue]]: ...

    @classmethod
    @overload
    def as_record_format(
        cls,
        table: PendingTable,
        allow_waiting_api_call: Literal[False],
    ) -> list[dict[str, CellValue]]: ...

    @classmethod
    def as_record_format(
        cls,
        table: PendingTable,
        allow_waiting_api_call: bool = True,
    ) -> list[dict[str, PendingCellValue]] | list[dict[str, CellValue]]:
        rows: list[dict[str, PendingCellValue]] = []
        row_indices: dict[str, int] = {}
        for (row, col), value in table.items():
            if value is WaitingAPICall and not allow_waiting_api_call:
                value = None
            if row not in row_indices:
                row_indices[row] = len(rows)
                rows.append({col: value})
            else:
                rows[row_indices[row]][col] = value
        return rows

    def _as_cell(self, value: PendingCellValue) -> str | Spinner:
        if isinstance(value, WaitingAPICallClass):
            return Spinner(**self.spinner_args)
        elif isinstance(value, int):
            return f"{value:,}"
        elif isinstance(value, float):
            return f"{value:.2f}"
        elif value is None:
            return "-"
        return str(value)


class ProfileAssetCentricCommand(ProfileCommand):
    def __init__(self, print_warning: bool = True, skip_tracking: bool = False, silent: bool = False) -> None:
        super().__init__(print_warning, skip_tracking, silent)
        self.table_title = "Asset Centric Profile"
        self.aggregators: dict[str, AssetCentricAggregator] = {}

    class Columns:
        Resource = "Resource"
        Count = "Count"
        MetadataKeyCount = "Metadata Key Count"
        LabelCount = "Label Count"
        Transformation = "Transformations"

    def asset_centric(self, client: ToolkitClient, verbose: bool = False) -> list[dict[str, CellValue]]:
        self.aggregators.update(
            {
                agg.display_name: agg
                for agg in [
                    AssetAggregator(client),
                    EventAggregator(client),
                    FileAggregator(client),
                    TimeSeriesAggregator(client),
                    SequenceAggregator(client),
                    RelationshipAggregator(client),
                    LabelCountAggregator(client),
                ]
            }
        )
        return self.create_profile_table(client)

    def create_initial_table(self, client: ToolkitClient) -> PendingTable:
        table: dict[tuple[str, str], str | int | float | bool | None | WaitingAPICallClass] = {}
        for index, aggregator in self.aggregators.items():
            table[(index, self.Columns.Resource)] = aggregator.display_name
            table[(index, self.Columns.Count)] = WaitingAPICall
            if isinstance(aggregator, MetadataAggregator):
                table[(index, self.Columns.MetadataKeyCount)] = WaitingAPICall
            else:
                table[(index, self.Columns.MetadataKeyCount)] = None
            if isinstance(aggregator, LabelAggregator):
                table[(index, self.Columns.LabelCount)] = WaitingAPICall
            else:
                table[(index, self.Columns.LabelCount)] = None
            table[(index, self.Columns.Transformation)] = WaitingAPICall
        return table

    def call_api(self, row: str, col: str, client: ToolkitClient) -> Callable:
        aggregator = self.aggregators[row]
        if col == self.Columns.Count:
            return aggregator.count
        elif col == self.Columns.MetadataKeyCount and isinstance(aggregator, MetadataAggregator):
            return aggregator.metadata_key_count
        elif col == self.Columns.LabelCount and isinstance(aggregator, LabelAggregator):
            return aggregator.label_count
        elif col == self.Columns.Transformation:
            return aggregator.transformation_count
        raise ValueError(f"Unknown column: {col} for row: {row}")


class ProfileRawCommand(ProfileCommand):
    class Columns:
        RAW = "Raw"
        Rows = "Rows"
        Columns = "Columns"
        Transformation = "Transformation"
        Destination = "Destination"
        ConflictMode = "ConflictMode"

    profile_row_limit = 10_000  # The number of rows to profile to get the number of columns.
    # The actual limit is 1 million, we typically run this against 30 tables and that high limit
    # will cause 504 errors.
    profile_timeout_seconds = 60 * 4  # Timeout for the profiling operation in seconds,
    # This is the same ase the run/query maximum timeout.
    _index_split = "-"
    is_dynamic_table = True

    def __init__(self, print_warning: bool = True, skip_tracking: bool = False, silent: bool = False) -> None:
        super().__init__(print_warning, skip_tracking, silent)
        self.table_title = "RAW Profile"
        self.destination_type = ""
        self.client: ToolkitClient | None = None

    def raw(
        self,
        client: ToolkitClient,
        destination_type: str,
        verbose: bool = False,
    ) -> list[dict[str, CellValue]]:
        self.table_title = f"RAW Profile destination: {destination_type}"
        self.destination_type = destination_type
        return self.create_profile_table(client)

    def create_initial_table(self, client: ToolkitClient) -> PendingTable:
        table: PendingTable = {}
        existing_tables = self._get_existing_tables(client)
        transformations_by_raw_table = self._get_transformations_by_raw_table(client, self.destination_type)
        for raw_id, transformations in transformations_by_raw_table.items():
            is_missing = raw_id not in existing_tables
            missing_str = " (missing)" if is_missing else ""
            for transformation in transformations:
                index = f"{raw_id!s}{self._index_split}{transformation.id}"
                table[(index, self.Columns.RAW)] = f"{raw_id!s}{missing_str}"
                if is_missing:
                    table[(index, self.Columns.Rows)] = "N/A"
                    table[(index, self.Columns.Columns)] = "N/A"
                else:
                    table[(index, self.Columns.Rows)] = WaitingAPICall
                    table[(index, self.Columns.Columns)] = None
                table[(index, self.Columns.Transformation)] = f"{transformation.name} ({transformation.external_id})"
                table[(index, self.Columns.Destination)] = (
                    transformation.destination.type if transformation.destination else "Unknown"
                )
                table[(index, self.Columns.ConflictMode)] = transformation.conflict_mode
        return table

    def call_api(self, row: str, col: str, client: ToolkitClient) -> Callable:
        if col == self.Columns.Rows:
            raw_id = row.split(self._index_split)[0]
            if "." not in raw_id:
                raise ValueError(f"Database and table name are required for {row} in column {col}.")
            db_name, table_name = raw_id.split(".", 1)
            return partial(
                client.raw.profile,
                database=db_name,
                table=table_name,
                limit=self.profile_row_limit,
                timeout_seconds=self.profile_timeout_seconds,
            )
        raise ValueError(f"There are no API calls for {row} in column {col}.")

    def format_result(self, result: object, row: str, col: str) -> CellValue:
        if isinstance(result, int | float | bool | str) or result is None:
            return result
        elif isinstance(result, RawProfileResults):
            return result.row_count
        raise ValueError(f"Unknown result type: {type(result)} for {row} in column {col}.")

    def update_table(
        self,
        current_table: PendingTable,
        result: object,
        row: str,
        col: str,
    ) -> PendingTable:
        if not isinstance(result, RawProfileResults) or col != self.Columns.Rows:
            return current_table
        is_complete = result.is_complete and result.row_count < self.profile_row_limit
        new_table: PendingTable = {}
        for (r, c), value in current_table.items():
            if r == row and c == self.Columns.Rows:
                new_table[(r, c)] = result.row_count if is_complete else f"≥{result.row_count:,}"
            elif r == row and c == self.Columns.Columns:
                new_table[(r, c)] = result.column_count if is_complete else f"≥{result.column_count:,}"
            else:
                new_table[(r, c)] = value
        return new_table

    @classmethod
    def _get_existing_tables(cls, client: ToolkitClient) -> set[RawTable]:
        existing_tables: set[RawTable] = set()
        databases = client.raw.databases.list(limit=-1)
        for database in databases:
            if database.name is None:
                continue
            tables = client.raw.tables.list(db_name=database.name, limit=-1)
            for table in tables:
                if table.name is None:
                    continue
                existing_tables.add(RawTable(db_name=database.name, table_name=table.name))
        return existing_tables

    @classmethod
    def _get_transformations_by_raw_table(
        cls, client: ToolkitClient, destination_type: str
    ) -> dict[RawTable, list[Transformation]]:
        transformations = client.transformations.list(destination_type=destination_type)
        if destination_type == "assets":
            transformations.extend(client.transformations.list(destination_type="asset_hierarchy"))
        transformations_by_raw_table: dict[RawTable, list[Transformation]] = defaultdict(list)
        for transformation in transformations:
            if transformation.query is None:
                # No query means no source table.
                continue
            sources = get_transformation_sources(transformation.query)
            for source in sources:
                if isinstance(source, RawTable):
                    transformations_by_raw_table[source].append(transformation)
        return transformations_by_raw_table
