from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from typing import Literal

from rich.console import Console
from rich.table import Table

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.utils.cdf import label_count, metadata_key_counts

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


class ProfileCommand(ToolkitCommand):
    @classmethod
    def asset_centric(
        cls,
        client: ToolkitClient,
        verbose: bool = False,
    ) -> list[dict[str, str | int]]:
        aggregators: list[AssetCentricAggregator] = [
            AssetAggregator(client),
            EventAggregator(client),
            FileAggregator(client),
            TimeSeriesAggregator(client),
            SequenceAggregator(client),
        ]
        with Console().status("profiling asset-centric", spinner="aesthetic", speed=0.4) as _:
            with ThreadPoolExecutor() as executor:
                rows = list(executor.map(cls.process_aggregator, aggregators))

        table = Table(
            title="Asset Centric Profile",
            title_justify="left",
            show_header=True,
            header_style="bold magenta",
        )
        table.add_column("Resource")
        table.add_column("Count")
        table.add_column("Metadata Key Count")
        table.add_column("Label Count")
        for row in rows:
            table.add_row(*(f"{cell:,}" if isinstance(cell, int) else str(cell) for cell in row.values()))
        console = Console()
        console.print(table)
        return rows

    @staticmethod
    def process_aggregator(aggregator: AssetCentricAggregator) -> dict[str, str | int]:
        row: dict[str, str | int] = {
            "Resource": aggregator.display_name,
            "Count": aggregator.count(),
        }
        if isinstance(aggregator, MetadataAggregator):
            count: str | int = aggregator.metadata_key_count()
        else:
            count = "-"
        row["Metadata Key Count"] = count
        if isinstance(aggregator, LabelAggregator):
            count = aggregator.label_count()
        else:
            count = "-"
        row["Label Count"] = count
        return row
