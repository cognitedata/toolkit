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
from .selectors import AllChartsSelector, CanvasSelector, ChartExternalIdSelector, ChartOwnerSelector, ChartSelector


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
        elif isinstance(selector, ChartExternalIdSelector):
            external_id_set = set(selector.external_ids)
            selected_charts = ChartList([chart for chart in selected_charts if chart.external_id in external_id_set])
        else:
            raise ToolkitNotImplementedError(f"Unsupported selector type {type(selector).__name__!r} for ChartIO")

        if limit is not None:
            selected_charts = ChartList(selected_charts[:limit])
        for chunk in chunker_sequence(selected_charts, self.CHUNK_SIZE):
            yield Page(worker_id="main", items=chunk)

    def count(self, selector: ChartSelector) -> int | None:
        # There is no way to get the count of charts up front.
        return None

    def data_to_json_chunk(
        self, data_chunk: Sequence[Chart], selector: ChartSelector | None = None
    ) -> list[dict[str, JsonVal]]:
        self._populate_timeseries_id_cache(data_chunk)
        return [self._dump_resource(chart) for chart in data_chunk]

    def _populate_timeseries_id_cache(self, data_chunk: Sequence[Chart]) -> None:
        timeseries_ids: set[int] = set()
        for chart in data_chunk:
            for item in chart.data.time_series_collection or []:
                if item.ts_id is not None and item.ts_external_id is None:
                    # We only look-up the internalID if the externalId is missing
                    timeseries_ids.add(item.ts_id)
        if timeseries_ids:
            self.client.lookup.time_series.external_id(list(timeseries_ids))

    def _dump_resource(self, chart: Chart) -> dict[str, JsonVal]:
        dumped = chart.as_write().dump()
        if isinstance(data := dumped.get("data"), dict) and isinstance(
            collection := data.get("timeSeriesCollection"), list
        ):
            for item in collection:
                ts_id = item.pop("tsId", None)
                if ts_id and item.get("tsExternalId") is None:
                    # We only look-up the externalID if it is missing
                    ts_external_id = self.client.lookup.time_series.external_id(ts_id)
                    if ts_external_id is not None:
                        item["tsExternalId"] = ts_external_id
        return dumped

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

    def data_to_json_chunk(
        self, data_chunk: Sequence[IndustrialCanvas], selector: CanvasSelector | None = None
    ) -> list[dict[str, JsonVal]]:
        # Need to do lookup to get external IDs for all asset-centric resources.
        raise ToolkitNotImplementedError("Exporting canvases is not implemented yet.")

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> IndustrialCanvasApply:
        # Need to do lookup to get external IDs for all asset-centric resources.
        raise ToolkitNotImplementedError("Importing canvases is not implemented yet.")
