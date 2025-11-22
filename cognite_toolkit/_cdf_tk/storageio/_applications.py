from collections.abc import Iterable, Sequence

from cognite_toolkit._cdf_tk.client.data_classes.canvas import (
    IndustrialCanvas,
    IndustrialCanvasApply,
)
from cognite_toolkit._cdf_tk.client.data_classes.charts import Chart, ChartList, ChartWrite
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotImplementedError
from cognite_toolkit._cdf_tk.tk_warnings import MediumSeverityWarning
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from ._base import Page, UploadableStorageIO, UploadItem
from .selectors import (
    AllChartsSelector,
    CanvasExternalIdSelector,
    CanvasSelector,
    ChartExternalIdSelector,
    ChartOwnerSelector,
    ChartSelector,
)


class ChartIO(UploadableStorageIO[ChartSelector, Chart, ChartWrite]):
    KIND = "Charts"
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".ndjson"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})
    SUPPORTED_READ_FORMATS = frozenset({".ndjson"})
    CHUNK_SIZE = 10
    BASE_SELECTOR = ChartSelector
    UPLOAD_ENDPOINT_TYPE = "app"
    UPLOAD_ENDPOINT_METHOD = "PUT"
    UPLOAD_ENDPOINT = "/storage/charts/charts"

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

    def json_chunk_to_data(self, data_chunk: list[tuple[str, dict[str, JsonVal]]]) -> Sequence[UploadItem[ChartWrite]]:
        self._populate_timeseries_external_id_cache([item_json for _, item_json in data_chunk])
        return super().json_chunk_to_data(data_chunk)

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> ChartWrite:
        return self._load_resource(item_json)

    def _populate_timeseries_external_id_cache(self, item_jsons: Sequence[dict[str, JsonVal]]) -> None:
        timeseries_external_ids: set[str] = set()
        for item_json in item_jsons:
            if isinstance(data := item_json.get("data"), dict) and isinstance(
                collection := data.get("timeSeriesCollection"), list
            ):
                for item in collection:
                    if not isinstance(item, dict):
                        continue
                    ts_external_id = item.get("tsExternalId")
                    if isinstance(ts_external_id, str):
                        timeseries_external_ids.add(ts_external_id)
        if timeseries_external_ids:
            self.client.lookup.time_series.id(list(timeseries_external_ids))

    def _load_resource(self, item_json: dict[str, JsonVal]) -> ChartWrite:
        if isinstance(data := item_json.get("data"), dict) and isinstance(
            collection := data.get("timeSeriesCollection"), list
        ):
            for item in collection:
                if not isinstance(item, dict):
                    continue
                ts_external_id = item.get("tsExternalId")
                if isinstance(ts_external_id, str) and item.get("tsId") is None:
                    # We only look-up the internalID if it is missing
                    ts_id = self.client.lookup.time_series.id(ts_external_id)
                    if ts_id is not None:
                        item["tsId"] = ts_id
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
        if not isinstance(selector, CanvasExternalIdSelector):
            raise ToolkitNotImplementedError(f"Unsupported selector type {type(selector).__name__!r} for CanvasIO")
        canvas_ids = selector.external_ids
        if limit is not None and len(canvas_ids) > limit:
            canvas_ids = canvas_ids[:limit]

        for chunk in chunker_sequence(canvas_ids, self.CHUNK_SIZE):
            items: list[IndustrialCanvas] = []
            for canvas_id in chunk:
                canvas = self.client.canvas.industrial.retrieve(canvas_id)
                if canvas is not None:
                    items.append(canvas)
                else:
                    MediumSeverityWarning("Canvas with external ID {canvas_id!r} not found. Skipping.").print_warning(
                        console=self.client.console
                    )
            yield Page(worker_id="main", items=items)

    def count(self, selector: CanvasSelector) -> int | None:
        if not isinstance(selector, CanvasExternalIdSelector):
            raise ToolkitNotImplementedError(f"Unsupported selector type {type(selector).__name__!r} for CanvasIO")
        return len(selector.external_ids)

    def data_to_json_chunk(
        self, data_chunk: Sequence[IndustrialCanvas], selector: CanvasSelector | None = None
    ) -> list[dict[str, JsonVal]]:
        self._populate_id_cache(data_chunk)
        return [self._dump_resource(canvas) for canvas in data_chunk]

    def _populate_id_cache(self, data_chunk: Sequence[IndustrialCanvas]) -> None:
        """Populate the client's lookup cache with all referenced resources in the canvases."""
        asset_ids: set[int] = set()
        time_series_ids: set[int] = set()
        event_ids: set[int] = set()
        file_ids: set[int] = set()
        for canvas in data_chunk:
            for container_ref in canvas.container_references:
                if container_ref.container_reference_type == "asset":
                    asset_ids.add(container_ref.resource_id)
                elif container_ref.container_reference_type == "timeseries":
                    time_series_ids.add(container_ref.resource_id)
                elif container_ref.container_reference_type == "event":
                    event_ids.add(container_ref.resource_id)
                elif container_ref.container_reference_type == "file":
                    file_ids.add(container_ref.resource_id)
        if asset_ids:
            self.client.lookup.assets.external_id(list(asset_ids))
        if time_series_ids:
            self.client.lookup.time_series.external_id(list(time_series_ids))
        if event_ids:
            self.client.lookup.events.external_id(list(event_ids))
        if file_ids:
            self.client.lookup.files.external_id(list(file_ids))

    def _dump_resource(self, canvas: IndustrialCanvas) -> dict[str, JsonVal]:
        dumped = canvas.as_write().dump()
        references = dumped.get("containerReferences", [])
        if not isinstance(references, list):
            return dumped
        for container_ref in references:
            if not isinstance(container_ref, dict):
                continue
            resource_id = container_ref.pop("resourceId", None)
            if not isinstance(resource_id, int):
                continue
            reference_type = container_ref.get("containerReferenceType")
            if reference_type == "asset":
                external_id = self.client.lookup.assets.external_id(resource_id)
            elif reference_type == "timeseries":
                external_id = self.client.lookup.time_series.external_id(resource_id)
            elif reference_type == "event":
                external_id = self.client.lookup.events.external_id(resource_id)
            elif reference_type == "file":
                external_id = self.client.lookup.files.external_id(resource_id)
            else:
                continue
            if external_id is not None:
                container_ref["resourceExternalId"] = external_id
        return dumped

    def json_chunk_to_data(
        self, data_chunk: list[tuple[str, dict[str, JsonVal]]]
    ) -> Sequence[UploadItem[IndustrialCanvasApply]]:
        self._populate_external_id_cache([item_json for _, item_json in data_chunk])
        return super().json_chunk_to_data(data_chunk)

    def _populate_external_id_cache(self, item_jsons: Sequence[dict[str, JsonVal]]) -> None:
        """Populate the client's lookup cache with all referenced resources in the canvases."""
        asset_external_ids: set[str] = set()
        time_series_external_ids: set[str] = set()
        event_external_ids: set[str] = set()
        file_external_ids: set[str] = set()
        for item_json in item_jsons:
            references = item_json.get("containerReferences", [])
            if not isinstance(references, list):
                continue
            for container_ref in references:
                if not isinstance(container_ref, dict):
                    continue
                resource_external_id = container_ref.get("resourceExternalId")
                if not isinstance(resource_external_id, str):
                    continue
                reference_type = container_ref.get("containerReferenceType")
                if reference_type == "asset":
                    asset_external_ids.add(resource_external_id)
                elif reference_type == "timeseries":
                    time_series_external_ids.add(resource_external_id)
                elif reference_type == "event":
                    event_external_ids.add(resource_external_id)
                elif reference_type == "file":
                    file_external_ids.add(resource_external_id)
        if asset_external_ids:
            self.client.lookup.assets.id(list(asset_external_ids))
        if time_series_external_ids:
            self.client.lookup.time_series.id(list(time_series_external_ids))
        if event_external_ids:
            self.client.lookup.events.id(list(event_external_ids))
        if file_external_ids:
            self.client.lookup.files.id(list(file_external_ids))

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> IndustrialCanvasApply:
        return self._load_resource(item_json)

    def _load_resource(self, item_json: dict[str, JsonVal]) -> IndustrialCanvasApply:
        references = item_json.get("containerReferences", [])
        if not isinstance(references, list):
            return IndustrialCanvasApply._load(item_json)
        for container_ref in references:
            if not isinstance(container_ref, dict):
                continue
            resource_external_id = container_ref.get("resourceExternalId")
            if not isinstance(resource_external_id, str):
                continue
            reference_type = container_ref.get("containerReferenceType")
            if reference_type == "asset":
                resource_id = self.client.lookup.assets.id(resource_external_id)
            elif reference_type == "timeseries":
                resource_id = self.client.lookup.time_series.id(resource_external_id)
            elif reference_type == "event":
                resource_id = self.client.lookup.events.id(resource_external_id)
            elif reference_type == "file":
                resource_id = self.client.lookup.files.id(resource_external_id)
            else:
                continue
            if resource_id is not None:
                container_ref["resourceId"] = resource_id
        return IndustrialCanvasApply._load(item_json)
