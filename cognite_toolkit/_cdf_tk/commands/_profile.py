from abc import ABC, abstractmethod
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Literal

from cognite.client.exceptions import CogniteException
from rich.live import Live
from rich.spinner import Spinner
from rich.table import Table

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.utils.cdf import (
    label_aggregate_count,
    label_count,
    metadata_key_counts,
    relationship_aggregate_count,
)

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
