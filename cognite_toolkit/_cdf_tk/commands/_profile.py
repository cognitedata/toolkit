from abc import abstractmethod

from rich.console import Console
from rich.table import Table

from cognite_toolkit._cdf_tk.client import ToolkitClient

from ._base import ToolkitCommand


class AssetCentricAggregator:
    def __init__(self, client: ToolkitClient) -> None:
        self.client = client

    @property
    @abstractmethod
    def display_name(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def count(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def metadata_key_count(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def label_count(self) -> int:
        raise NotImplementedError


class AssetAggregator(AssetCentricAggregator):
    @property
    def display_name(self) -> str:
        return "Assets"

    def count(self) -> int:
        return self.client.assets.aggregate_count()

    def metadata_key_count(self) -> int:
        return 0

    def label_count(self) -> int:
        return 0


class ProfileCommand(ToolkitCommand):
    @classmethod
    def asset_centric(
        cls,
        client: ToolkitClient,
        verbose: bool = False,
    ) -> None:
        rows = []
        for aggregator in [AssetAggregator(client)]:
            rows.append(
                {
                    "Resource": aggregator.display_name,
                    "Count": aggregator.count(),
                    "Metadata Key Count": aggregator.metadata_key_count(),
                    "Label Count": aggregator.label_count(),
                }
            )
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
