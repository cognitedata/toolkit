from collections.abc import Iterable, Sequence
from itertools import zip_longest
from typing import Any

from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    HTTPResult,
    RequestMessage,
    SuccessResponse,
)
from cognite_toolkit._cdf_tk.client.http_client._item_classes import ItemsResultList
from cognite_toolkit._cdf_tk.client.resource_classes.legacy.canvas import (
    IndustrialCanvas,
    IndustrialCanvasApply,
)
from cognite_toolkit._cdf_tk.client.resource_classes.legacy.charts import Chart, ChartList, ChartWrite
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotImplementedError
from cognite_toolkit._cdf_tk.tk_warnings import HighSeverityWarning, MediumSeverityWarning
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
    """Download and upload Industrial Canvases to/from CDF.

    Args:
        client (ToolkitClient): The Cognite Toolkit client to use for API interactions.
        exclude_existing_version (bool): Whether to exclude the 'existingVersion' field when uploading canvases.
            Defaults to True. If you set this to False, the upload may fail if the existing version in CDF is
            lower or equal to the one in the uploaded data.
    """

    KIND = "IndustrialCanvas"
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".ndjson"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})
    SUPPORTED_READ_FORMATS = frozenset({".ndjson"})
    CHUNK_SIZE = 10
    BASE_SELECTOR = CanvasSelector

    def __init__(self, client: ToolkitClient, exclude_existing_version: bool = True) -> None:
        super().__init__(client)
        self.exclude_existing_version = exclude_existing_version

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

    def upload_items(
        self,
        data_chunk: Sequence[UploadItem[IndustrialCanvasApply]],
        http_client: HTTPClient,
        selector: CanvasSelector | None = None,
    ) -> ItemsResultList:
        config = http_client.config
        results = ItemsResultList()
        for item in data_chunk:
            instances = item.item.as_instances()
            upsert_items: list[dict[str, JsonValue]] = []
            for instance in instances:
                dumped = instance.dump()
                if self.exclude_existing_version:
                    dumped.pop("existingVersion", None)
                upsert_items.append(dumped)

            canvas = item.item.canvas
            existing = self.client.canvas.industrial.retrieve(canvas.external_id)
            if existing is not None:
                existing_instance_ids = existing.as_write().as_instance_ids(include_solution_tags=False)
                delete_set = set(existing_instance_ids) - set(item.item.as_instance_ids())
                to_delete: list[Any] = [
                    {
                        "space": instance_id.space,
                        "externalId": instance_id.external_id,
                        "instanceType": instance_id.instance_type,
                    }
                    for instance_id in delete_set
                ]
            else:
                to_delete = []

            last_response: HTTPResult | None = None
            for upsert_chunk, delete_chunk in zip_longest(
                chunker_sequence(upsert_items, 1000), chunker_sequence(to_delete, 1000), fillvalue=None
            ):
                body_content: dict[str, JsonValue] = {}
                if upsert_chunk:
                    # MyPy fails do understand that list[dict[str, JsonValue]] is a subtype of JsonValue
                    body_content["items"] = upsert_chunk  # type: ignore[assignment]
                if delete_chunk:
                    body_content["delete"] = delete_chunk

                response = http_client.request_single_retries(
                    message=RequestMessage(
                        endpoint_url=config.create_api_url("/models/instances"),
                        method="POST",
                        body_content=body_content,
                    )
                )
                if not isinstance(response, SuccessResponse):
                    results.append(response.as_item_response(item.source_id))
                last_response = response
            if last_response is not None:
                results.append(last_response.as_item_response(item.source_id))
        return results

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
        dumped = canvas.as_write().dump(keep_existing_version=False)
        references = dumped.get("containerReferences", [])
        if not isinstance(references, list):
            return dumped
        new_container_references: list[Any] = []
        for container_ref in references:
            if not isinstance(container_ref, dict):
                new_container_references.append(container_ref)
                continue
            sources = container_ref.get("sources", [])
            if not isinstance(sources, list) or len(sources) == 0:
                new_container_references.append(container_ref)
                continue
            source = sources[0]
            if not isinstance(source, dict) or "properties" not in source:
                new_container_references.append(container_ref)
                continue
            properties = source["properties"]
            if not isinstance(properties, dict):
                new_container_references.append(container_ref)
                continue
            reference_type = properties.get("containerReferenceType")
            if (
                reference_type
                in {
                    "charts",
                    "dataGrid",
                }
            ):  # These container reference types are special cases with a resourceId statically set to -1, which is why we skip them
                new_container_references.append(container_ref)
                continue
            resource_id = properties.pop("resourceId", None)
            if not isinstance(resource_id, int):
                HighSeverityWarning(
                    f"Invalid resourceId {resource_id!r} in Canvas {canvas.canvas.name}. Skipping."
                ).print_warning(console=self.client.console)
                continue
            if reference_type == "asset":
                external_id = self.client.lookup.assets.external_id(resource_id)
            elif reference_type == "timeseries":
                external_id = self.client.lookup.time_series.external_id(resource_id)
            elif reference_type == "event":
                external_id = self.client.lookup.events.external_id(resource_id)
            elif reference_type == "file":
                external_id = self.client.lookup.files.external_id(resource_id)
            else:
                new_container_references.append(container_ref)
                continue
            if external_id is None:
                HighSeverityWarning(
                    f"Failed to look-up {reference_type} external ID for resource ID {resource_id!r}. Skipping resource in Canvas {canvas.canvas.name}"
                ).print_warning(console=self.client.console)
                continue
            properties["resourceExternalId"] = external_id
            new_container_references.append(container_ref)
        dumped["containerReferences"] = new_container_references
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
                sources = container_ref.get("sources", [])
                if not isinstance(sources, list) or len(sources) == 0:
                    continue
                source = sources[0]
                if not isinstance(source, dict) or "properties" not in source:
                    continue
                properties = source["properties"]
                if not isinstance(properties, dict):
                    continue

                resource_external_id = properties.get("resourceExternalId")
                if not isinstance(resource_external_id, str):
                    continue

                reference_type = properties.get("containerReferenceType")
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
        name = self._get_name(item_json)
        references = item_json.get("containerReferences", [])
        if not isinstance(references, list):
            return IndustrialCanvasApply._load(item_json)
        new_container_references: list[Any] = []
        for container_ref in references:
            if not isinstance(container_ref, dict):
                new_container_references.append(container_ref)
                continue
            sources = container_ref.get("sources", [])
            if not isinstance(sources, list) or len(sources) == 0:
                new_container_references.append(container_ref)
                continue
            source = sources[0]
            if not isinstance(source, dict) or "properties" not in source:
                new_container_references.append(container_ref)
                continue
            properties = source["properties"]
            if not isinstance(properties, dict):
                new_container_references.append(container_ref)
                continue
            resource_external_id = properties.pop("resourceExternalId", None)
            if not isinstance(resource_external_id, str):
                new_container_references.append(container_ref)
                continue
            reference_type = properties.get("containerReferenceType")
            if reference_type == "asset":
                resource_id = self.client.lookup.assets.id(resource_external_id)
            elif reference_type == "timeseries":
                resource_id = self.client.lookup.time_series.id(resource_external_id)
            elif reference_type == "event":
                resource_id = self.client.lookup.events.id(resource_external_id)
            elif reference_type == "file":
                resource_id = self.client.lookup.files.id(resource_external_id)
            else:
                new_container_references.append(container_ref)
                continue
            if resource_id is None:
                # Failed look-up, skip the resourceId setting
                HighSeverityWarning(
                    f"Failed to look-up {reference_type} ID for external ID {resource_external_id!r}. Skipping resource in Canvas {name}"
                ).print_warning(console=self.client.console)
                continue
            properties["resourceId"] = resource_id
            new_container_references.append(container_ref)
        new_item = dict(item_json)
        new_item["containerReferences"] = new_container_references

        return IndustrialCanvasApply._load(new_item)

    @classmethod
    def _get_name(cls, item_json: dict[str, JsonVal]) -> str:
        try:
            return item_json["canvas"]["sources"][0]["properties"]["name"]  # type: ignore[index,return-value, call-overload]
        except (KeyError, IndexError, TypeError):
            return "<unknown>"
