from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from rich.console import Console

from cognite_toolkit._cdf_tk.client.data_classes.charts import ChartList, ChartWriteList
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotImplementedError
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from ._base import StorageIO, StorageIOConfig, T_Selector


@dataclass(frozen=True)
class ChartSelector: ...


@dataclass(frozen=True)
class ChartUserSelector(ChartSelector):
    owner_id: str


@dataclass(frozen=True)
class AllChartSelector(ChartSelector): ...


@dataclass(frozen=True)
class ChartFileSelector(ChartSelector):
    filepath: Path


class ChartIO(StorageIO[ChartSelector, ChartWriteList, ChartList]):
    folder_name = "cdf_applications"
    kind = "Charts"
    display_name = "CDF Charts"
    supported_download_formats = frozenset({".ndjson"})
    supported_compressions = frozenset({".gz"})
    supported_read_formats = frozenset({".ndjson"})
    chunk_size = 10

    def download_iterable(self, selector: ChartSelector, limit: int | None = None) -> Iterable[ChartList]:
        selected_charts = self.client.charts.list(visibility="PUBLIC")
        if isinstance(selector, AllChartSelector):
            ...
        elif isinstance(selector, ChartUserSelector):
            selected_charts = ChartList([chart for chart in selected_charts if chart.owner_id == selector.owner_id])
        else:
            raise ToolkitNotImplementedError("Unsupported selector type for ChartIO")
        for chunk in chunker_sequence(selected_charts, self.chunk_size):
            for chart in chunk:
                for ts_ref in chart.data.time_series_collection or []:
                    if ts_ref.ts_external_id is None and ts_ref.ts_id is not None:
                        # Ensure that the externalID is populated for the Chart. This is needed in-case
                        # the chart is uploaded in another CDF project.
                        ts_ref.ts_external_id = self.client.lookup.time_series.external_id(ts_ref.ts_id)
            yield chunk

    def count(self, selector: ChartSelector) -> int | None:
        # There is no way to get the count of charts up front.
        return None

    def upload_items(self, data_chunk: ChartWriteList, selector: ChartSelector) -> None:
        # Todo validate all references exist in CDF before uploading.
        self.client.charts.upsert(data_chunk)

    def data_to_json_chunk(self, data_chunk: ChartList) -> list[dict[str, JsonVal]]:
        return [chart.as_write().dump() for chart in data_chunk]

    def json_chunk_to_data(self, data_chunk: list[dict[str, JsonVal]]) -> ChartWriteList:
        return ChartWriteList._load(data_chunk)

    def configurations(self, selector: ChartSelector) -> Iterable[StorageIOConfig]:
        # Charts dose not have any configurations.
        return []

    def load_selector(self, datafile: Path) -> ChartSelector:
        return ChartFileSelector(filepath=datafile)

    def ensure_configurations(self, selector: T_Selector, console: Console | None = None) -> None:
        # Charts do not have any configurations to ensure.
        return None
