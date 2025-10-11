from collections.abc import Iterable

from cognite_toolkit._cdf_tk.client.data_classes.canvas import (
    IndustrialCanvas,
    IndustrialCanvasApply,
    IndustrialCanvasApplyList,
    IndustrialCanvasList,
)
from cognite_toolkit._cdf_tk.client.data_classes.charts import Chart, ChartList, ChartWrite, ChartWriteList
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotImplementedError
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from ._base import StorageIO
from .selectors import AllChartsSelector, CanvasSelector, ChartOwnerSelector, ChartSelector


class ChartIO(StorageIO[str, ChartSelector, ChartWriteList, ChartList]):
    FOLDER_NAME = "cdf_application_data"
    KIND = "Charts"
    DISPLAY_NAME = "CDF Charts"
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".ndjson"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})
    SUPPORTED_READ_FORMATS = frozenset({".ndjson"})
    CHUNK_SIZE = 10

    def as_id(self, item: dict[str, JsonVal] | object) -> str:
        if isinstance(item, dict) and isinstance(item.get("externalId"), str):
            # MyPy checked above.
            return item["externalId"]  # type: ignore[return-value]
        if isinstance(item, ChartWrite | Chart):
            return item.external_id
        raise TypeError(f"Cannot extract ID from item of type {type(item).__name__!r}")

    def stream_data(self, selector: ChartSelector, limit: int | None = None) -> Iterable[ChartList]:
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
            yield chunk

    def count(self, selector: ChartSelector) -> int | None:
        # There is no way to get the count of charts up front.
        return None

    def data_to_json_chunk(self, data_chunk: ChartList) -> list[dict[str, JsonVal]]:
        return [chart.as_write().dump() for chart in data_chunk]

    def json_chunk_to_data(self, data_chunk: list[dict[str, JsonVal]]) -> ChartWriteList:
        return ChartWriteList._load(data_chunk)


class CanvasIO(StorageIO[str, CanvasSelector, IndustrialCanvasApplyList, IndustrialCanvasList]):
    FOLDER_NAME = "cdf_application_data"
    KIND = "IndustrialCanvas"
    DISPLAY_NAME = "CDF Industrial Canvases"
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".ndjson"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})
    SUPPORTED_READ_FORMATS = frozenset({".ndjson"})
    CHUNK_SIZE = 10

    @staticmethod
    def _get_id_from_dict(item: dict[str, JsonVal] | object) -> str | None:
        if not isinstance(item, dict):
            return None
        if "canvas" not in item:
            return None
        canvas = item["canvas"]
        if not isinstance(canvas, dict):
            return None
        external_id = canvas.get("externalId")
        if not isinstance(external_id, str):
            return None
        return external_id

    def as_id(self, item: dict[str, JsonVal] | object) -> str:
        if canvas_id := self._get_id_from_dict(item):
            return canvas_id
        if isinstance(item, IndustrialCanvas | IndustrialCanvasApply):
            return item.as_id()
        raise TypeError(f"Cannot extract ID from item of type {type(item).__name__!r}")

    def stream_data(self, selector: CanvasSelector, limit: int | None = None) -> Iterable[IndustrialCanvasList]:
        raise ToolkitNotImplementedError("Streaming canvases is not implemented yet.")

    def count(self, selector: CanvasSelector) -> int | None:
        raise ToolkitNotImplementedError("Counting canvases is not implemented yet.")

    def data_to_json_chunk(self, data_chunk: IndustrialCanvasList) -> list[dict[str, JsonVal]]:
        # Need to do lookup to get external IDs for all asset-centric resources.
        raise ToolkitNotImplementedError("Exporting canvases is not implemented yet.")

    def json_chunk_to_data(self, data_chunk: list[dict[str, JsonVal]]) -> IndustrialCanvasApplyList:
        # Need to do lookup to get external IDs for all asset-centric resources.
        raise ToolkitNotImplementedError("Importing canvases is not implemented yet.")
