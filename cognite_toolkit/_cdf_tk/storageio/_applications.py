from collections.abc import Iterable, Sequence

from cognite_toolkit._cdf_tk.client.data_classes.canvas import (
    IndustrialCanvas,
    IndustrialCanvasApply,
)
from cognite_toolkit._cdf_tk.client.data_classes.charts import Chart, ChartList, ChartWrite
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotImplementedError
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from ._base import Page, UploadableStorageIO
from .selectors import AllChartsSelector, CanvasSelector, ChartOwnerSelector, ChartSelector


class ChartIO(UploadableStorageIO[ChartSelector, Chart, ChartWrite]):
    KIND = "Charts"
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".ndjson"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})
    SUPPORTED_READ_FORMATS = frozenset({".ndjson"})
    CHUNK_SIZE = 10
    BASE_SELECTOR = ChartSelector

    def as_id(self, item: Chart) -> str:
        return item.external_id

    def stream_data(self, selector: ChartSelector, limit: int | None = None) -> Iterable[Page]:
        selected_charts = self.client.charts.list(visibility="PUBLIC")
        if isinstance(selector, AllChartsSelector):
            ...
        elif isinstance(selector, ChartOwnerSelector):
            selected_charts = ChartList([chart for chart in selected_charts if chart.owner_id == selector.owner_id])
        else:
            raise ToolkitNotImplementedError(f"Unsupported selector type {type(selector).__name__!r} for ChartIO")

        if limit is not None:
            selected_charts = ChartList(selected_charts[:limit])
        for chunk in chunker_sequence(selected_charts, self.CHUNK_SIZE):
            ts_ids_to_lookup = {
                ts_ref.ts_id
                for chart in chunk
                for ts_ref in chart.data.time_series_collection or []
                if ts_ref.ts_external_id is None and ts_ref.ts_id is not None
            }

            if ts_ids_to_lookup:
                retrieved_ts = self.client.time_series.retrieve_multiple(
                    ids=list(ts_ids_to_lookup), ignore_unknown_ids=True
                )
                id_to_external_id = {ts.id: ts.external_id for ts in retrieved_ts}

                for chart in chunk:
                    for ts_ref in chart.data.time_series_collection or []:
                        if ts_ref.ts_id in id_to_external_id:
                            ts_ref.ts_external_id = id_to_external_id[ts_ref.ts_id]
            yield Page(worker_id="main", items=chunk)

    def count(self, selector: ChartSelector) -> int | None:
        # There is no way to get the count of charts up front.
        return None

    def data_to_json_chunk(self, data_chunk: Sequence[Chart]) -> list[dict[str, JsonVal]]:
        return [chart.as_write().dump() for chart in data_chunk]

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> ChartWrite:
        return ChartWrite._load(item_json)


class CanvasIO(UploadableStorageIO[CanvasSelector, IndustrialCanvas, IndustrialCanvasApply]):
    KIND = "IndustrialCanvas"
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".ndjson"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})
    SUPPORTED_READ_FORMATS = frozenset({".ndjson"})
    CHUNK_SIZE = 10
    BASE_SELECTOR = CanvasSelector

    def as_id(self, item: IndustrialCanvas) -> str:
        return item.as_id()

    def stream_data(self, selector: CanvasSelector, limit: int | None = None) -> Iterable[Page]:
        raise ToolkitNotImplementedError("Streaming canvases is not implemented yet.")

    def count(self, selector: CanvasSelector) -> int | None:
        raise ToolkitNotImplementedError("Counting canvases is not implemented yet.")

    def data_to_json_chunk(self, data_chunk: Sequence[IndustrialCanvas]) -> list[dict[str, JsonVal]]:
        # Need to do lookup to get external IDs for all asset-centric resources.
        raise ToolkitNotImplementedError("Exporting canvases is not implemented yet.")

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> IndustrialCanvasApply:
        # Need to do lookup to get external IDs for all asset-centric resources.
        raise ToolkitNotImplementedError("Importing canvases is not implemented yet.")
