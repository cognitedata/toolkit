import itertools
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Mapping
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
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.utils import humanize_collection
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
from cognite_toolkit._cdf_tk.utils.sql_parser import SQLParser, SQLTable

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
                    executor.submit(self.create_api_callable(row, col, client)): (row, col)
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
    def create_api_callable(self, row: str, col: str, client: ToolkitClient) -> Callable:
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


class ProfileAssetCommand(ProfileCommand):
    def __init__(self, print_warning: bool = True, skip_tracking: bool = False, silent: bool = False) -> None:
        super().__init__(print_warning, skip_tracking, silent)
        self.table_title = "Asset Profile for Hierarchy"
        self.hierarchy: str | None = None
        self.aggregators: dict[str, MetadataAggregator] = {}

    class Columns:
        Resource = "Resource"
        Count = "Count"
        DataSets = "DataSet"
        DataSetCount = "DataSet Count"
        Transformations = "Transformation"
        RawTable = "Raw Table"
        RowCount = "Rows"
        ColumnCount = "Columns"

    is_dynamic_table = True
    _index_split = "-"
    profile_row_limit = 10_000  # The number of rows to profile to get the number of columns.
    # The actual limit is 1 million, we typically run this against 30 tables and that high limit
    # will cause 504 errors.
    profile_timeout_seconds = 60 * 4  # Timeout for the profiling operation in seconds,

    def assets(
        self, client: ToolkitClient, hierarchy: str | None = None, verbose: bool = False
    ) -> list[dict[str, CellValue]]:
        """
        Profile assets in the given hierarchy.
        This method will create a table with the count of assets, events, files, timeseries, sequences,
        relationships, and labels in the specified hierarchy.
        """
        if hierarchy is None:
            raise NotImplementedError("Interactive mode is not implemented yet. Please provide a hierarchy.")
        self.hierarchy = hierarchy
        self.table_title = f"Asset Profile for Hierarchy: {hierarchy}"
        self.aggregators = {
            agg.display_name: agg
            for agg in [
                AssetAggregator(client),
                EventAggregator(client),
                FileAggregator(client),
                TimeSeriesAggregator(client),
                SequenceAggregator(client),
            ]
        }
        return self.create_profile_table(client)

    def create_initial_table(self, client: ToolkitClient) -> PendingTable:
        table: PendingTable = {}
        for index, aggregator in self.aggregators.items():
            table[(index, self.Columns.Resource)] = aggregator.display_name
            table[(index, self.Columns.Count)] = WaitingAPICall
            table[(index, self.Columns.DataSets)] = None
            table[(index, self.Columns.DataSetCount)] = None
            table[(index, self.Columns.Transformations)] = None
            table[(index, self.Columns.RawTable)] = None
            table[(index, self.Columns.RowCount)] = None
            table[(index, self.Columns.ColumnCount)] = None
        return table

    def create_api_callable(self, row: str, col: str, client: ToolkitClient) -> Callable:
        agg_name, *rest = row.split(self._index_split)
        aggregator = self.aggregators[agg_name]
        if col == self.Columns.Count:
            return partial(aggregator.count, hierarchy=self.hierarchy)
        elif col == self.Columns.DataSets:
            return partial(aggregator.used_data_sets, hierarchy=self.hierarchy)
        elif col == self.Columns.DataSetCount:
            data_set_external_id = rest[-1] if len(rest) > 0 else None
            if not data_set_external_id:
                raise ValueError(f"DataSet external ID is required for {row} in column {col}.")
            return partial(aggregator.count, data_set_external_id=data_set_external_id, hierarchy=self.hierarchy)
        elif col == self.Columns.Transformations:
            data_set_external_id = rest[-1] if len(rest) > 0 else None
            if not data_set_external_id:
                raise ValueError(f"DataSet external ID is required for {row} in column {col}.")
            return partial(aggregator.used_transformations, data_set_external_ids=[data_set_external_id])
        elif col == self.Columns.RowCount:
            db_table = rest[-1] if len(rest) > 0 else None
            if not isinstance(db_table, str) or "." not in db_table:
                raise ValueError(f"Database and table name are required for {row} in column {col} in {rest}.")
            db_name, table_name = db_table.split(".")
            return partial(
                client.raw.profile,
                database=db_name,
                table=table_name,
                limit=self.profile_row_limit,
                timeout_seconds=self.profile_timeout_seconds,
            )
        raise ValueError(f"Unexpected API Call for row {row} and column {col}.")

    def format_result(self, result: object, row: str, col: str) -> CellValue:
        if isinstance(result, int | float | bool | str):
            return result
        elif col == self.Columns.DataSets:
            return result[0] if isinstance(result, list) and result and isinstance(result[0], str) else None
        elif col == self.Columns.Transformations:
            if isinstance(result, list) and len(result) > 0 and isinstance(result[0], Transformation):
                return f"{result[0].name} ({result[0].external_id})"
            return None
        elif col == self.Columns.RowCount:
            if isinstance(result, RawProfileResults):
                return result.row_count
            return None
        raise ValueError(f"unexpected result type {type(result)} for row {row} and column {col}.")

    def update_table(
        self,
        current_table: PendingTable,
        result: object,
        row: str,
        col: str,
    ) -> PendingTable:
        handlers: Mapping[str, Callable[[PendingTable, object, str], PendingTable]] = {
            self.Columns.Count: self._update_count,
            self.Columns.DataSets: self._update_datasets,
            self.Columns.DataSetCount: self._update_dataset_count,
            self.Columns.Transformations: self._update_transformations,
            self.Columns.RowCount: self._update_row_count,
        }
        handler = handlers.get(col)
        if handler:
            return handler(current_table, result, row)
        return current_table

    def _update_count(
        self,
        current_table: PendingTable,
        result: object,
        selected_row: str,
    ) -> PendingTable:
        new_table: PendingTable = {}
        for (row, col), value in current_table.items():
            if row == selected_row and col == self.Columns.DataSets:
                new_table[(row, col)] = WaitingAPICall
            else:
                new_table[(row, col)] = value
        return new_table

    def _update_datasets(
        self,
        current_table: PendingTable,
        result: object,
        selected_row: str,
    ) -> PendingTable:
        if not (isinstance(result, list) and len(result) > 0 and all(isinstance(item, str) for item in result)):
            return current_table
        new_table: PendingTable = {}
        for (row, col), value in current_table.items():
            if row != selected_row:
                new_table[(row, col)] = value
                continue
            for data_set in result:
                new_index = f"{row}{self._index_split}{data_set}"
                if col == self.Columns.DataSetCount:
                    new_table[(new_index, col)] = WaitingAPICall
                elif col == self.Columns.DataSets:
                    new_table[(new_index, col)] = data_set
                else:
                    new_table[(new_index, col)] = value
        return new_table

    def _update_dataset_count(
        self,
        current_table: PendingTable,
        result: object,
        selected_row: str,
    ) -> PendingTable:
        if not isinstance(result, int):
            return current_table
        new_table: PendingTable = {}
        for (row, col), value in current_table.items():
            if row != selected_row:
                new_table[(row, col)] = value
                continue
            if col == self.Columns.Transformations:
                new_table[(row, col)] = WaitingAPICall
            else:
                new_table[(row, col)] = value
        return new_table

    def _update_transformations(
        self,
        current_table: PendingTable,
        result: object,
        selected_row: str,
    ) -> PendingTable:
        if not (
            isinstance(result, list) and len(result) > 0 and all(isinstance(item, Transformation) for item in result)
        ):
            return current_table
        sources_by_transformation_id = {
            transformation.id: [
                s for s in get_transformation_sources(transformation.query or "") if isinstance(s, RawTable)
            ]
            for transformation in result
        }
        new_table: PendingTable = {}
        for (row, col), value in current_table.items():
            if row != selected_row:
                new_table[(row, col)] = value
                continue
            for transformation in result:
                sources = sources_by_transformation_id[transformation.id]
                if not sources:
                    new_table[(row, col)] = value
                    continue
                for source in sources:
                    new_index = f"{row}{self._index_split}{source!s}"
                    if col == self.Columns.RawTable:
                        new_table[(new_index, col)] = str(source)
                    elif col == self.Columns.RowCount:
                        new_table[(new_index, col)] = WaitingAPICall
                    elif col == self.Columns.ColumnCount:
                        new_table[(new_index, col)] = None
                    elif col == self.Columns.Transformations:
                        new_table[(new_index, col)] = f"{transformation.name} ({transformation.external_id})"
                    else:
                        new_table[(new_index, col)] = value
        return new_table

    def _update_row_count(
        self,
        current_table: PendingTable,
        result: object,
        selected_row: str,
    ) -> PendingTable:
        if not isinstance(result, RawProfileResults):
            return current_table
        new_table: PendingTable = {}
        for (row, col), value in current_table.items():
            if row != selected_row:
                new_table[(row, col)] = value
                continue
            is_complete = result.is_complete and result.row_count < self.profile_row_limit
            if col == self.Columns.RowCount:
                new_table[(row, col)] = result.row_count if is_complete else f"≥{result.row_count:,}"
            elif col == self.Columns.ColumnCount:
                new_table[(row, col)] = result.column_count if is_complete else f"≥{result.column_count:,}"
            else:
                new_table[(row, col)] = value
        return new_table


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

    def create_api_callable(self, row: str, col: str, client: ToolkitClient) -> Callable:
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


class ProfileTransformationCommand(ProfileCommand):
    valid_destinations: frozenset[str] = frozenset({"assets", "files", "events", "timeseries", "sequences"})

    def __init__(self, print_warning: bool = True, skip_tracking: bool = False, silent: bool = False) -> None:
        super().__init__(print_warning, skip_tracking, silent)
        self.table_title = "Transformation Profile"
        self.destination_type: Literal["assets", "files", "events", "timeseries", "sequences"] | None = None

    class Columns:
        Transformation = "Transformation"
        Source = "Sources"
        DestinationColumns = "Destination Columns"
        Destination = "Destination"
        ConflictMode = "Conflict Mode"
        IsPaused = "Is Paused"

    def transformation(
        self, client: ToolkitClient, destination_type: str | None = None, verbose: bool = False
    ) -> list[dict[str, CellValue]]:
        self.destination_type = self._validate_destination_type(destination_type)
        return self.create_profile_table(client)

    @classmethod
    def _validate_destination_type(
        cls, destination_type: str | None
    ) -> Literal["assets", "files", "events", "timeseries", "sequences"]:
        if destination_type is None or destination_type not in cls.valid_destinations:
            raise ToolkitValueError(
                f"Invalid destination type: {destination_type}. Must be one of {humanize_collection(cls.valid_destinations)}."
            )
        # We validated the destination type above
        return destination_type  # type: ignore[return-value]

    def create_initial_table(self, client: ToolkitClient) -> dict[tuple[str, str], PendingCellValue]:
        if self.valid_destinations is None:
            raise ToolkitValueError("Destination type must be set before calling create_initial_table.")
        iterable: Iterable[Transformation] = client.transformations.list(
            destination_type=self.destination_type, limit=-1
        )
        if self.destination_type == "assets":
            iterable = itertools.chain(iterable, client.transformations(destination_type="asset_hierarchy", limit=-1))
        table: dict[tuple[str, str], PendingCellValue] = {}
        for transformation in iterable:
            sources: list[SQLTable] = []
            destination_columns: list[str] = []
            if transformation.query:
                parser = SQLParser(transformation.query, operation="Profile transformations")
                sources = parser.sources
                destination_columns = parser.destination_columns
            index = str(transformation.id)
            table[(index, self.Columns.Transformation)] = transformation.name or transformation.external_id or "Unknown"
            table[(index, self.Columns.Source)] = ", ".join(map(str, sources))
            table[(index, self.Columns.DestinationColumns)] = (
                ", ".join(destination_columns) or None if destination_columns else None
            )
            table[(index, self.Columns.Destination)] = (
                transformation.destination.type or "Unknown" if transformation.destination else "Unknown"
            )
            table[(index, self.Columns.ConflictMode)] = transformation.conflict_mode or "Unknown"
            table[(index, self.Columns.IsPaused)] = (
                str(transformation.schedule.is_paused) if transformation.schedule else "No schedule"
            )
        return table

    def create_api_callable(self, row: str, col: str, client: ToolkitClient) -> Callable:
        raise NotImplementedError(f"{type(self).__name__} does not support API calls for {col} in row {row}.")
