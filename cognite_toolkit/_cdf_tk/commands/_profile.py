import itertools
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Literal

from cognite.client.data_classes import Transformation
from cognite.client.exceptions import CogniteAPIError, CogniteException, CogniteReadTimeout
from rich.console import Console
from rich.live import Live
from rich.spinner import Spinner
from rich.table import Table

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.raw import RawTable
from cognite_toolkit._cdf_tk.utils.cdf import (
    get_transformation_sources,
    label_aggregate_count,
    label_count,
    metadata_key_counts,
    raw_row_count,
    relationship_aggregate_count,
)
from cognite_toolkit._cdf_tk.utils.sql_parser import SQLParser, SQLTable

from ._base import ToolkitCommand


class AssetCentricAggregator(ABC):
    def __init__(self, client: ToolkitClient) -> None:
        self.client = client

    @property
    @abstractmethod
    def display_name(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def count(self) -> int:
        raise NotImplementedError


class MetadataAggregator(AssetCentricAggregator, ABC):
    def __init__(
        self, client: ToolkitClient, resource_name: Literal["assets", "events", "files", "timeseries", "sequences"]
    ) -> None:
        super().__init__(client)
        self.resource_name = resource_name

    def metadata_key_count(self) -> int:
        return len(metadata_key_counts(self.client, self.resource_name))


class LabelAggregator(MetadataAggregator, ABC):
    def label_count(self) -> int:
        return len(label_count(self.client, self.resource_name))


class AssetAggregator(LabelAggregator):
    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client, "assets")

    @property
    def display_name(self) -> str:
        return "Assets"

    def count(self) -> int:
        return self.client.assets.aggregate_count()


class EventAggregator(MetadataAggregator):
    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client, "events")

    @property
    def display_name(self) -> str:
        return "Events"

    def count(self) -> int:
        return self.client.events.aggregate_count()


class FileAggregator(LabelAggregator):
    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client, "files")

    @property
    def display_name(self) -> str:
        return "Files"

    def count(self) -> int:
        response = self.client.files.aggregate()
        if response:
            return response[0].count
        else:
            return 0


class TimeSeriesAggregator(MetadataAggregator):
    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client, "timeseries")

    @property
    def display_name(self) -> str:
        return "TimeSeries"

    def count(self) -> int:
        return self.client.time_series.aggregate_count()


class SequenceAggregator(MetadataAggregator):
    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client, "sequences")

    @property
    def display_name(self) -> str:
        return "Sequences"

    def count(self) -> int:
        return self.client.sequences.aggregate_count()


class RelationshipAggregator(AssetCentricAggregator):
    @property
    def display_name(self) -> str:
        return "Relationships"

    def count(self) -> int:
        results = relationship_aggregate_count(self.client)
        return sum(result.count for result in results)


class LabelCountAggregator(AssetCentricAggregator):
    @property
    def display_name(self) -> str:
        return "Labels"

    def count(self) -> int:
        return label_aggregate_count(self.client)


class ProfileCommand(ToolkitCommand):
    class Columns:
        Resource = "Resource"
        Count = "Count"
        MetadataKeyCount = "Metadata Key Count*"
        LabelCount = "Label Count*"

    columns = (
        Columns.Resource,
        Columns.Count,
        Columns.MetadataKeyCount,
        Columns.LabelCount,
    )
    spinner_speed = 1.0

    @classmethod
    def asset_centric(
        cls,
        client: ToolkitClient,
        verbose: bool = False,
    ) -> list[dict[str, str]]:
        aggregators: list[AssetCentricAggregator] = [
            AssetAggregator(client),
            EventAggregator(client),
            FileAggregator(client),
            TimeSeriesAggregator(client),
            SequenceAggregator(client),
            RelationshipAggregator(client),
            LabelCountAggregator(client),
        ]
        results, api_calls = cls._create_initial_table(aggregators)
        with Live(cls.create_profile_table(results), refresh_per_second=4) as live:
            with ThreadPoolExecutor(max_workers=8) as executor:
                future_to_cell = {
                    executor.submit(api_calls[(index, col)]): (index, col)
                    for index in range(len(aggregators))
                    for col in cls.columns
                    if (index, col) in api_calls
                }
                for future in as_completed(future_to_cell):
                    index, col = future_to_cell[future]
                    results[index][col] = future.result()
                    live.update(cls.create_profile_table(results))
        return [{col: str(value) for col, value in row.items()} for row in results]

    @classmethod
    def _create_initial_table(
        cls, aggregators: list[AssetCentricAggregator]
    ) -> tuple[list[dict[str, str | Spinner]], dict[tuple[int, str], Callable[[], str]]]:
        rows: list[dict[str, str | Spinner]] = []
        api_calls: dict[tuple[int, str], Callable[[], str]] = {}
        for index, aggregator in enumerate(aggregators):
            row: dict[str, str | Spinner] = {
                cls.Columns.Resource: aggregator.display_name,
                cls.Columns.Count: Spinner("arc", text="loading...", style="bold green", speed=cls.spinner_speed),
            }
            api_calls[(index, cls.Columns.Count)] = cls._call_api(aggregator.count)
            count: str | Spinner = "-"
            if isinstance(aggregator, MetadataAggregator):
                count = Spinner("arc", text="loading...", style="bold green", speed=cls.spinner_speed)
                api_calls[(index, cls.Columns.MetadataKeyCount)] = cls._call_api(aggregator.metadata_key_count)
            row[cls.Columns.MetadataKeyCount] = count

            count = "-"
            if isinstance(aggregator, LabelAggregator):
                count = Spinner("arc", text="loading...", style="bold green", speed=cls.spinner_speed)
                api_calls[(index, cls.Columns.LabelCount)] = cls._call_api(aggregator.label_count)
            row[cls.Columns.LabelCount] = count
            rows.append(row)
        return rows, api_calls

    @classmethod
    def create_profile_table(cls, rows: list[dict[str, str | Spinner]]) -> Table:
        table = Table(
            title="Asset Centric Profile",
            title_justify="left",
            show_header=True,
            header_style="bold magenta",
        )
        table.add_column(cls.Columns.Resource)
        table.add_column(cls.Columns.Count)
        table.add_column("Metadata Key Count*")
        table.add_column("Label Count*")
        for row in rows:
            table.add_row(*row.values())
        return table

    @staticmethod
    def _call_api(call_fun: Callable[[], int]) -> Callable[[], str]:
        def styled_callable() -> str:
            try:
                value = call_fun()
            except CogniteException as e:
                return type(e).__name__
            else:
                return f"{value:,}"

        return styled_callable


class ProfileRawCommand(ToolkitCommand):
    class Columns:
        RAW = "Raw"
        Rows = "Rows"
        Columns = "Columns"
        Transformation = "Transformation"
        Destination = "Destination"
        Operation = "Operation"
        UseAll = "Use All"

    columns = (
        Columns.RAW,
        Columns.Rows,
        Columns.Columns,
        Columns.Transformation,
        Columns.Destination,
        Columns.Operation,
        Columns.UseAll,
    )
    spinner_args: Mapping[str, Any] = dict(name="arc", text="loading...", style="bold green", speed=1.0)

    profile_row_limit = 10_000  # The number of rows to profile to get the number of columns.
    # The actual limit is 1 million, we typically run this against 30 tables and that high limit
    # will cause 504 errors.
    profile_timeout_seconds = 60 * 4  # Timeout for the profiling operation in seconds,
    # This is the same ase the run/query maximum timeout.

    @classmethod
    def raw(
        cls,
        client: ToolkitClient,
        destination_type: str,
        verbose: bool = False,
    ) -> list[dict[str, str]]:
        console = Console()
        with console.status("Preparing...", spinner="aesthetic", speed=0.4) as _:
            existing_tables = cls._get_existing_tables(client)
            transformations_by_raw_table = cls._get_transformations_by_raw_table(client, destination_type)

            table_content, api_calls, indices_by_raw_table = cls._setup_table_and_api_calls(
                client, transformations_by_raw_table, existing_tables
            )
        with Live(cls.draw_table(table_content, destination_type), refresh_per_second=4, console=console) as live:
            with ThreadPoolExecutor(max_workers=4) as executor:
                future_to_cell = {
                    executor.submit(api_calls[(raw_id, col)]): (raw_id, col)
                    for raw_id in transformations_by_raw_table.keys()
                    for col in [cls.Columns.Rows, cls.Columns.Columns]
                    if (raw_id, col) in api_calls
                }
                for future in as_completed(future_to_cell):
                    raw_id, col = future_to_cell[future]
                    count = future.result()
                    index = indices_by_raw_table[raw_id]
                    table_content[index][col] = count
                    live.update(cls.draw_table(table_content, destination_type))
        return [{col: str(value) for col, value in row.items()} for row in table_content]

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

    @classmethod
    def _setup_table_and_api_calls(
        cls,
        client: ToolkitClient,
        transformations_by_raw_table: dict[RawTable, list[Transformation]],
        existing_tables: set[RawTable],
    ) -> tuple[list[dict[str, str | Spinner]], dict[tuple[RawTable, str], Callable[[], str]], dict[RawTable, int]]:
        rows: list[dict[str, str | Spinner]] = []
        api_calls: dict[tuple[RawTable, str], Callable[[], str]] = {}
        index_by_raw_id: dict[RawTable, int] = {}
        index = 0
        for raw_id, transformations in transformations_by_raw_table.items():
            is_existing = raw_id in existing_tables
            if is_existing:
                api_calls[(raw_id, cls.Columns.Rows)] = cls.row_count(client, raw_id)
                api_calls[(raw_id, cls.Columns.Columns)] = cls.column_count(client, raw_id)
            for no, transformation in enumerate(transformations):
                is_first = no == 0
                existing_str = " (missing)" if not is_existing else ""
                row: dict[str, str | Spinner] = {
                    cls.Columns.RAW: f"{raw_id.db_name}.{raw_id.table_name}{existing_str}" if is_first else "",
                    cls.Columns.Rows: Spinner(**cls.spinner_args) if is_first and is_existing else "",
                    cls.Columns.Columns: Spinner(**cls.spinner_args) if is_first and is_existing else "",
                    cls.Columns.Transformation: transformation.name or transformation.external_id or "Unknown",
                    cls.Columns.Destination: transformation.destination.type
                    if transformation.destination and transformation.destination.type
                    else "Unknown",
                    cls.Columns.Operation: transformation.conflict_mode or "Unknown",
                    cls.Columns.UseAll: str("where" in (transformation.query or "unknown").casefold()),
                }
                rows.append(row)
                if is_first:
                    index_by_raw_id[raw_id] = index
                index += 1
        return rows, api_calls, index_by_raw_id

    @classmethod
    def draw_table(cls, rows: list[dict[str, str | Spinner]], destination: str) -> Table:
        table = Table(
            title=f"RAW Profile destination: {destination}",
            title_justify="left",
            show_header=True,
            header_style="bold magenta",
        )
        for col in cls.columns:
            table.add_column(col)
        for row in rows:
            table.add_row(*row.values())
        return table

    @classmethod
    def column_count(cls, client: ToolkitClient, raw_table: RawTable) -> Callable[[], str]:
        def api_call() -> str:
            try:
                # MyPy does not understand that ToolkitClient.raw.profile exists, it fails to account for the override
                # in the init of the ToolkitClient class.
                result = client.raw.profile(  # type: ignore[attr-defined]
                    raw_table, limit=cls.profile_row_limit, timeout_seconds=cls.profile_timeout_seconds
                )
            except CogniteAPIError as e1:
                return f"{type(e1).__name__}({e1.code})"
            except CogniteReadTimeout:
                return f"Read timeout {cls.profile_timeout_seconds} exceeded"
            except CogniteException as e3:
                return type(e3).__name__
            else:
                output = f"{result.column_count:,}"
                if not result.is_complete or result.row_count >= cls.profile_row_limit:
                    output = "≥" + output
                return output

        return api_call

    @classmethod
    def row_count(cls, client: ToolkitClient, raw_table: RawTable) -> Callable[[], str]:
        def api_call() -> str:
            try:
                count = raw_row_count(client, raw_table)
            except CogniteAPIError as e1:
                return f"{type(e1).__name__}({e1.code})"
            except CogniteException as e2:
                return type(e2).__name__
            else:
                return f"{count:,}"

        return api_call


class ProfileTransformationCommand(ToolkitCommand):
    class Columns:
        Transformation = "Transformation"
        Source = "Sources"
        DestinationColumns = "Destination Columns"
        Destination = "Destination"
        ConflictMode = "Conflict Mode"
        IsPaused = "Is Paused"

    columns = (
        Columns.Transformation,
        Columns.Source,
        Columns.DestinationColumns,
        Columns.Destination,
        Columns.ConflictMode,
        Columns.IsPaused,
    )

    @classmethod
    def transformation(
        cls,
        client: ToolkitClient,
        destination_type: str,
        verbose: bool = False,
    ) -> list[dict[str, str]]:
        console = Console()
        content: list[dict[str, str]] = []
        with console.status("Loading transformations...", spinner="aesthetic", speed=0.4) as _:
            iterable: Iterable[Transformation] = client.transformations.list(destination_type=destination_type)
            if destination_type == "assets":
                iterable = itertools.chain(iterable, client.transformations(destination_type="asset_hierarchy"))
            for transformation in iterable:
                sources: list[SQLTable] = []
                destination_columns: list[str] = []
                if transformation.query:
                    parser = SQLParser(transformation.query, operation="Profile transformations")
                    sources = parser.sources
                    destination_columns = parser.destination_columns
                row: dict[str, str] = {
                    cls.Columns.Transformation: transformation.name or transformation.external_id or "Unknown",
                    cls.Columns.Source: ", ".join(map(str, sources)),
                    cls.Columns.DestinationColumns: ", ".join(destination_columns) or "None",
                    cls.Columns.Destination: transformation.destination.type or "Unknown"
                    if transformation.destination
                    else "Unknown",
                    cls.Columns.ConflictMode: transformation.conflict_mode or "Unknown",
                    cls.Columns.IsPaused: str(transformation.schedule.is_paused)
                    if transformation.schedule
                    else "No schedule",
                }
                content.append(row)

        table = cls.draw_table(content, destination_type)
        console.print(table)
        return content

    @classmethod
    def draw_table(
        cls,
        rows: list[dict[str, str]],
        destination: str,
    ) -> Table:
        table = Table(
            title=f"Transformation Profile destination: {destination}",
            title_justify="left",
            show_header=True,
            header_style="bold magenta",
        )
        for col in cls.columns:
            table.add_column(col)
        for row in rows:
            table.add_row(*row.values())
        return table
