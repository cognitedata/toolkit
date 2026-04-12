from collections.abc import Callable, Iterable, Sequence
from itertools import chain
from typing import Any, TypeVar

from cognite.client.data_classes.data_modeling import EdgeId

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.api.instances import INSTANCE_DELETE_ENDPOINT, INSTANCE_UPSERT_ENDPOINT
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    RequestMessage,
    ToolkitAPIError,
)
from cognite_toolkit._cdf_tk.client.http_client._item_classes import ItemsRequest, ItemsResultList, ItemsResultMessage
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, InstanceDefinitionId, NodeId
from cognite_toolkit._cdf_tk.client.request_classes.filters import ChartMonitorJobFilter
from cognite_toolkit._cdf_tk.client.resource_classes.canvas import (
    CANVAS_INSTANCE_SPACE,
    IndustrialCanvasRequest,
    IndustrialCanvasResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.chart import ChartRequest, ChartResponse
from cognite_toolkit._cdf_tk.client.resource_classes.chart_monitoring_job import (
    ChartMonitoringJobRequest,
    ChartMonitoringJobResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.chart_scheduled_calculation import (
    ChartScheduledCalculationRequest,
    ChartScheduledCalculationResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.charts_data import MonitoringJobReference
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotImplementedError
from cognite_toolkit._cdf_tk.tk_warnings import HighSeverityWarning
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from ._base import Bookmark, DataItem, Page, UploadableStorageIO
from .logger import LogIssue
from .progress import NoBookmark
from .selectors import (
    AllChartsSelector,
    CanvasExternalIdSelector,
    CanvasSelector,
    ChartExternalIdSelector,
    ChartOwnerSelector,
    ChartSelector,
)

TChartBackendRequest = TypeVar(
    "TChartBackendRequest",
    ChartMonitoringJobRequest,
    ChartScheduledCalculationRequest,
)
TChartBackendResponse = TypeVar(
    "TChartBackendResponse",
    ChartMonitoringJobResponse,
    ChartScheduledCalculationResponse,
)


class ChartIO(UploadableStorageIO[ChartSelector, ChartResponse, ChartRequest]):
    KIND = "Charts"
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".ndjson"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})
    SUPPORTED_READ_FORMATS = frozenset({".ndjson"})
    CHUNK_SIZE = 10
    BASE_SELECTOR = ChartSelector
    UPLOAD_ENDPOINT_TYPE = "app"
    UPLOAD_ENDPOINT_METHOD = "PUT"
    UPLOAD_ENDPOINT = "/storage/charts/charts"
    UPDATE_ENDPOINT = "/storage/charts/charts/{externalId}"

    def __init__(self, client: ToolkitClient, skip_existing: bool = False, skip_backend_services: bool = False) -> None:
        super().__init__(client)
        # We need to store existing charts as we use different endpoints depending on whether
        # the chart exist or not. Note this scales O(n) and not O(1) with memory wrt to number of Charts.
        # However, we know that there are only a few 1000s Charts at most, thus this should not be a problem.
        # and is cheaper than doing a lookup for each chart we are about to deploy.
        self._existing_charts: set[str] | None = None
        self._skip_existing = skip_existing
        self._skip_backend_services = skip_backend_services

    @property
    def existing_charts(self) -> set[str]:
        if self._existing_charts is None:
            self._existing_charts = {chart.external_id for chart in self.client.charts.list()}
        return self._existing_charts

    def stream_data(
        self,
        selector: ChartSelector,
        limit: int | None = None,
        bookmark: Bookmark | None = None,
    ) -> Iterable[Page]:
        selected_charts = self.client.charts.list(visibility=None)
        self._existing_charts = {chart.external_id for chart in selected_charts}
        if isinstance(selector, AllChartsSelector):
            ...
        elif isinstance(selector, ChartOwnerSelector):
            selected_charts = [chart for chart in selected_charts if chart.owner_id == selector.owner_id]
        elif isinstance(selector, ChartExternalIdSelector):
            external_id_set = set(selector.external_ids)
            selected_charts = [chart for chart in selected_charts if chart.external_id in external_id_set]
        else:
            raise ToolkitNotImplementedError(f"Unsupported selector type {type(selector).__name__!r} for ChartIO")

        if limit is not None:
            selected_charts = selected_charts[:limit]

        for chunk in chunker_sequence(selected_charts, self.CHUNK_SIZE):
            if not self._skip_backend_services:
                self._download_backend_services(chunk)
            yield Page(
                worker_id="main",
                items=[DataItem(tracking_id=item.external_id, item=item) for item in chunk],
                bookmark=NoBookmark(),
            )

    def _download_backend_services(self, chunk: list[ChartResponse]) -> None:
        """Downloads the backend services monitoring jobs and scheduled calculations for each Chart."""
        calculations = self.client.charts.scheduled_calculations.retrieve(
            [
                ExternalId(external_id=calculation.id)
                for chart in chunk
                for calculation in chart.data.scheduled_calculation_collection or []
                if calculation.id
            ],
            ignore_unknown_ids=True,
        )
        calculation_by_id = {calc.external_id: calc for calc in calculations}
        monitoring_job_by_id: dict[int, ChartMonitoringJobResponse] = {}
        all_job_ids = [job.id for chart in chunk for job in chart.data.monitoring_jobs or [] if job.id is not None]
        for job_chunk in chunker_sequence(all_job_ids, 100):
            monitoring_jobs = self.client.charts.monitoring_jobs.list(ChartMonitorJobFilter(ids=job_chunk), limit=100)
            monitoring_job_by_id.update({job.id: job for job in monitoring_jobs})

        for chart in chunk:
            if chart.data.scheduled_calculation_collection is not None:
                chart.scheduled_calculations = [
                    calculation_by_id[calculation.id]
                    for calculation in chart.data.scheduled_calculation_collection
                    if calculation.id in calculation_by_id
                ]
            if chart.data.monitoring_jobs is not None:
                chart.monitoring_jobs = [
                    monitoring_job_by_id[job.id] for job in chart.data.monitoring_jobs if job.id in monitoring_job_by_id
                ]

    def count(self, selector: ChartSelector) -> int | None:
        if isinstance(selector, ChartExternalIdSelector):
            return len(selector.external_ids)
        elif isinstance(selector, AllChartsSelector):
            return len(self.existing_charts)
        return None

    def data_to_json_chunk(
        self, data_chunk: Page[ChartResponse], selector: ChartSelector | None = None
    ) -> Page[dict[str, JsonVal]]:
        self._populate_timeseries_id_cache([item.item for item in data_chunk.items])
        result = [
            DataItem(tracking_id=item.tracking_id, item=self._dump_resource(item.item)) for item in data_chunk.items
        ]
        return data_chunk.create_from(result)

    def _populate_timeseries_id_cache(self, data_chunk: Sequence[ChartResponse]) -> None:
        timeseries_ids: set[int] = set()
        for chart in data_chunk:
            for item in chart.data.time_series_collection or []:
                if item.ts_id is not None and item.ts_external_id is None:
                    # We only look-up the internalID if the externalId is missing
                    timeseries_ids.add(item.ts_id)
        if timeseries_ids:
            self.client.lookup.time_series.external_id(list(timeseries_ids))

    def _dump_resource(self, chart: ChartResponse) -> dict[str, JsonVal]:
        request = chart.as_request_resource()
        dumped = request.dump()
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
        if request.monitoring_jobs is not None:
            dumped["monitoringJobs"] = [job.dump() for job in request.monitoring_jobs]
        if request.scheduled_calculations is not None:
            dumped["scheduledCalculations"] = [calculation.dump() for calculation in request.scheduled_calculations]
        return dumped

    def json_chunk_to_data(self, data_chunk: Page[dict[str, JsonVal]]) -> Page[ChartRequest]:
        self._populate_timeseries_external_id_cache([item.item for item in data_chunk.items])
        return super().json_chunk_to_data(data_chunk)

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> ChartRequest:
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

    def _load_resource(self, item_json: dict[str, JsonVal]) -> ChartRequest:
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
        return ChartRequest._load(item_json)

    def upload_items(
        self,
        data_chunk: Page[ChartRequest],
        http_client: HTTPClient,
        selector: ChartSelector | None = None,
    ) -> ItemsResultList:
        to_create: list[DataItem[ChartRequest]] = []
        to_update: list[DataItem[ChartRequest]] = []
        for item in data_chunk.items:
            if item.item.external_id in self.existing_charts and not self._skip_existing:
                to_update.append(item)
            elif item.item.external_id not in self.existing_charts:
                to_create.append(item)
            else:
                self.logger.tracker.finalize_item(item.tracking_id, "skipped")

        if not self._skip_backend_services:
            failed_charts = self._upload_backend_services(to_create + to_update)
            to_create = [item for item in to_create if item.item.external_id not in failed_charts]
            to_update = [item for item in to_update if item.item.external_id not in failed_charts]

        results = ItemsResultList()
        if to_create:
            results.extend(self._create_charts(to_create, http_client))
        if to_update:
            results.extend(self._update_charts(to_update, http_client))
        return results

    def _create_charts(self, to_create: list[DataItem[ChartRequest]], http_client: HTTPClient) -> ItemsResultList:
        url = http_client.config.create_app_url(self.UPLOAD_ENDPOINT)
        return http_client.request_items_retries(
            message=ItemsRequest(
                endpoint_url=url,
                method="PUT",
                items=to_create,
                extra_body_fields=dict(self.UPLOAD_EXTRA_ARGS or {}),
            )
        )

    def _update_charts(
        self, to_update: list[DataItem[ChartRequest]], http_client: HTTPClient
    ) -> Iterable[ItemsResultMessage]:
        for item in to_update:
            chart = item.item
            url = http_client.config.create_app_url(self.UPDATE_ENDPOINT.format(externalId=chart.external_id))
            dumped = chart.dump()
            # The endpoint requires that externalId is not part of the body. Note that
            # it is already set as a path variable.
            dumped.pop("externalId", None)
            update_request = RequestMessage(endpoint_url=url, method="PUT", body_content=dumped)
            item_response = http_client.request_single_retries(update_request)
            yield item_response.as_item_response(item.tracking_id)

    @staticmethod
    def _collect_unique_backend_requests(
        items: list[DataItem[ChartRequest]],
        get_requests: Callable[[ChartRequest], Sequence[TChartBackendRequest] | None],
        duplicate_label: str,
    ) -> tuple[dict[ExternalId, tuple[TChartBackendRequest, str]], list[LogIssue]]:
        by_external_id: dict[ExternalId, tuple[TChartBackendRequest, str]] = {}
        log_entries: list[LogIssue] = []
        for item in items:
            for request in get_requests(item.item) or []:
                ext_id = request.as_id()
                if ext_id in by_external_id:
                    log_entries.append(
                        LogIssue(
                            id=item.tracking_id,
                            message=f"Duplicated {duplicate_label} ID {ext_id} in chart {item.item.external_id}",
                        )
                    )
                else:
                    by_external_id[ext_id] = request, item.tracking_id
        return by_external_id, log_entries

    def _upsert_unique_backend_requests(
        self,
        unique_by_id: dict[ExternalId, tuple[TChartBackendRequest, str]],
        existing_ids: set[ExternalId],
        update: Callable[[Sequence[TChartBackendRequest]], list[TChartBackendResponse]],
        create: Callable[[Sequence[TChartBackendRequest]], list[TChartBackendResponse]],
        resource_kind: str,
    ) -> tuple[list[LogIssue], set[ExternalId], dict[ExternalId, TChartBackendResponse]]:
        log_entries: list[LogIssue] = []
        failed: set[ExternalId] = set()
        succeeded: dict[ExternalId, TChartBackendResponse] = {}
        for ext_id, (request, tracking_id) in unique_by_id.items():
            if ext_id in existing_ids:
                try:
                    updated = update([request])[0]
                except ToolkitAPIError as e:
                    log_entries.append(
                        LogIssue(id=tracking_id, message=f"Failed to update {resource_kind} {ext_id}: {e}")
                    )
                    failed.add(ext_id)
                except IndexError:
                    log_entries.append(
                        LogIssue(
                            id=tracking_id,
                            message=f"Failed to update {resource_kind} {ext_id}. No response returned.",
                        )
                    )
                    failed.add(ext_id)
                else:
                    succeeded[ext_id] = updated
            else:
                if request.nonce == "<missing>":
                    # Do not know how to deal with `nonce`. Creating from the service principal is risky as that
                    # most likely grants too broad access.
                    raise NotImplementedError(f"Do not support creating {resource_kind} for {ext_id}")
                try:
                    created = create([request])[0]
                except ToolkitAPIError as e:
                    log_entries.append(
                        LogIssue(id=tracking_id, message=f"Failed to create {resource_kind} {ext_id}: {e}")
                    )
                    failed.add(ext_id)
                except IndexError:
                    log_entries.append(
                        LogIssue(
                            id=tracking_id,
                            message=f"Failed to create {resource_kind}. No response returned.",
                        )
                    )
                    failed.add(ext_id)
                else:
                    succeeded[ext_id] = created
        return log_entries, failed, succeeded

    def _upload_backend_services(self, items: list[DataItem[ChartRequest]]) -> set[str]:
        """Uploads the backend services monitoring jobs and scheduled calculations for each Chart. Returns a set of external IDs of Charts that failed to upload backend services."""
        log_entries: list[LogIssue] = []

        monitoring_job_by_id, monitoring_dup_logs = self._collect_unique_backend_requests(
            items, lambda chart: chart.monitoring_jobs, "monitoring job"
        )
        log_entries.extend(monitoring_dup_logs)

        calculation_by_id, calculation_dup_logs = self._collect_unique_backend_requests(
            items, lambda chart: chart.scheduled_calculations, "scheduled calculation"
        )
        log_entries.extend(calculation_dup_logs)

        existing_job_ids = {
            job.as_id()
            for job in self.client.charts.monitoring_jobs.retrieve(
                list(monitoring_job_by_id.keys()), ignore_unknown_ids=True
            )
        }
        job_upsert_logs, failed_jobs, succeeded_jobs = self._upsert_unique_backend_requests(
            monitoring_job_by_id,
            existing_job_ids,
            self.client.charts.monitoring_jobs.update,
            self.client.charts.monitoring_jobs.create,
            "monitoring job",
        )
        log_entries.extend(job_upsert_logs)

        existing_calculation_ids = {
            calculation.as_id()
            for calculation in self.client.charts.scheduled_calculations.retrieve(
                list(calculation_by_id.keys()), ignore_unknown_ids=True
            )
        }
        calculation_upsert_logs, failed_calculations, _ = self._upsert_unique_backend_requests(
            calculation_by_id,
            existing_calculation_ids,
            self.client.charts.scheduled_calculations.update,
            self.client.charts.scheduled_calculations.create,
            "scheduled calculation",
        )
        log_entries.extend(calculation_upsert_logs)

        upserted_job_by_internal_id: dict[int, ChartMonitoringJobResponse] = {}
        for ext_id, job_response in succeeded_jobs.items():
            orig_job, _tracking = monitoring_job_by_id[ext_id]
            upserted_job_by_internal_id[orig_job.id or job_response.id] = job_response

        failed_charts: set[str] = set()
        for item in items:
            chart = item.item
            job_failed = any(job.as_id() in failed_jobs for job in chart.monitoring_jobs or [])
            calculation_failed = any(
                calculation.as_id() in failed_calculations for calculation in chart.scheduled_calculations or []
            )
            if job_failed or calculation_failed:
                failed_charts.add(chart.external_id)
                if job_failed:
                    self.logger.tracker.add_issue(item.tracking_id, "Failed upserting monitoring jobs")
                if calculation_failed:
                    self.logger.tracker.add_issue(item.tracking_id, "Failed upserting scheduled calculations")
                self.logger.tracker.finalize_item(item.tracking_id, "failure")
                continue

            if chart.data.monitoring_jobs:
                new_references: list[MonitoringJobReference] = []
                for job_reference in chart.data.monitoring_jobs:
                    if job_reference.id is None or job_reference.id not in upserted_job_by_internal_id:
                        new_references.append(job_reference)
                        continue
                    upsert_job = upserted_job_by_internal_id[job_reference.id]
                    new_references.append(job_reference.model_copy(update={"id": upsert_job.id}))
                chart.data = chart.data.model_copy(update={"monitoring_jobs": new_references})

        if log_entries:
            self.logger.log(log_entries)
        return failed_charts


class CanvasIO(UploadableStorageIO[CanvasSelector, IndustrialCanvasResponse, IndustrialCanvasRequest]):
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

    def __init__(
        self, client: ToolkitClient, exclude_existing_version: bool = True, include_solution_tags: bool = True
    ) -> None:
        super().__init__(client)
        self.exclude_existing_version = exclude_existing_version
        self.include_solution_tags = include_solution_tags

    def stream_data(
        self,
        selector: CanvasSelector,
        limit: int | None = None,
        bookmark: Bookmark | None = None,
    ) -> Iterable[Page]:
        if not isinstance(selector, CanvasExternalIdSelector):
            raise ToolkitNotImplementedError(f"Unsupported selector type {type(selector).__name__!r} for CanvasIO")
        canvas_ids = selector.external_ids
        if limit is not None and len(canvas_ids) > limit:
            canvas_ids = canvas_ids[:limit]

        for chunk in chunker_sequence(canvas_ids, self.CHUNK_SIZE):
            items = self.client.canvas.retrieve(NodeId.from_str_ids(chunk, space=CANVAS_INSTANCE_SPACE))
            self._log_retrieve_issues(chunk, items)
            yield Page(
                worker_id="main",
                items=[DataItem(tracking_id=item.external_id, item=item) for item in items],
                bookmark=NoBookmark(),
            )

    def _log_retrieve_issues(self, chunk: tuple[str, ...], items: list[IndustrialCanvasResponse]) -> None:
        found = {item.external_id for item in items}
        not_found = sorted(set(chunk) - found)
        if not_found:
            self.logger.log(
                [
                    LogIssue(id=not_found_item, message=f"Did not find {not_found_item} in CDF")
                    for not_found_item in not_found
                ]
            )
            for not_found_item in not_found:
                self.logger.tracker.finalize_item(str(not_found_item), "failure")

    def count(self, selector: CanvasSelector) -> int | None:
        if not isinstance(selector, CanvasExternalIdSelector):
            raise ToolkitNotImplementedError(f"Unsupported selector type {type(selector).__name__!r} for CanvasIO")
        return len(selector.external_ids)

    def upload_items(
        self,
        data_chunk: Page[IndustrialCanvasRequest],
        http_client: HTTPClient,
        selector: CanvasSelector | None = None,
    ) -> ItemsResultList:
        existing_canvas = self.client.canvas.retrieve([item.item.as_id() for item in data_chunk.items])
        existing_by_ids = {item.external_id: item for item in existing_canvas}
        results = ItemsResultList()
        for item in data_chunk.items:
            item_id = item.item.as_id()
            instances = item.item.dump_instances(include_solution_tags=self.include_solution_tags)
            instance_ids = set(item.item.as_ids(include_solution_tags=self.include_solution_tags))
            edges_to_delete: list[EdgeId] = []
            nodes_to_delete: list[NodeId] = []
            if item_id.external_id in existing_by_ids:
                # We will never delete solution tags
                existing_ids = existing_by_ids[item_id.external_id].as_ids(include_solution_tags=False)
                for instance_id in existing_ids:
                    if instance_id not in instance_ids:
                        if isinstance(instance_id, EdgeId):
                            edges_to_delete.append(instance_id)
                        elif isinstance(instance_id, NodeId):
                            nodes_to_delete.append(instance_id)

            for instance_chunk in chunker_sequence(instances, INSTANCE_UPSERT_ENDPOINT.item_limit):
                result = http_client.request_single_retries(
                    message=RequestMessage(
                        endpoint_url=http_client.config.create_api_url(INSTANCE_UPSERT_ENDPOINT.path),
                        method=INSTANCE_UPSERT_ENDPOINT.method,
                        body_content={"items": instance_chunk},  # type:ignore[dict-item]
                    )
                )
                results.append(result.as_item_response(item.tracking_id))

            # It is possible to delete and create in the same request, but we keep this separate
            # as there is a risk for deadlocks.
            # We delete all edges before nodes to avoid a deadlock.
            delete_chunk: list[InstanceDefinitionId]
            for delete_chunk in chain(  # type: ignore[assignment]
                chunker_sequence(edges_to_delete, INSTANCE_DELETE_ENDPOINT.item_limit),
                chunker_sequence(nodes_to_delete, INSTANCE_DELETE_ENDPOINT.item_limit),
            ):
                result = http_client.request_single_retries(
                    message=RequestMessage(
                        endpoint_url=http_client.config.create_api_url(INSTANCE_DELETE_ENDPOINT.path),
                        method=INSTANCE_DELETE_ENDPOINT.method,
                        body_content={
                            "items": [instance_id.dump() for instance_id in delete_chunk],
                        },
                    )
                )
                results.append(result.as_item_response(item.tracking_id))
        return results

    def data_to_json_chunk(
        self, data_chunk: Page[IndustrialCanvasResponse], selector: CanvasSelector | None = None
    ) -> Page[dict[str, JsonVal]]:
        self._populate_id_cache([item.item for item in data_chunk.items])
        result = [
            DataItem(tracking_id=item.tracking_id, item=self._dump_resource(item.item)) for item in data_chunk.items
        ]
        return data_chunk.create_from(result)

    def _populate_id_cache(self, data_chunk: Sequence[IndustrialCanvasResponse]) -> None:
        """Populate the client's lookup cache with all referenced resources in the canvases."""
        asset_ids: set[int] = set()
        time_series_ids: set[int] = set()
        event_ids: set[int] = set()
        file_ids: set[int] = set()
        for canvas in data_chunk:
            for container_ref in canvas.container_references or []:
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

    def _dump_resource(self, canvas: IndustrialCanvasResponse) -> dict[str, JsonVal]:
        dumped = canvas.as_request_resource().dump()
        references = dumped.get("containerReferences", [])
        if not isinstance(references, list):
            return dumped
        new_container_references: list[Any] = []
        for container_ref in references:
            if not isinstance(container_ref, dict):
                new_container_references.append(container_ref)
                continue
            properties = self._get_properties(container_ref)
            if not isinstance(properties, dict):
                new_container_references.append(container_ref)
                continue
            reference_type = properties.get("containerReferenceType")
            if reference_type in {"charts", "dataGrid"}:
                new_container_references.append(container_ref)
                continue
            resource_id = properties.pop("resourceId", None)
            if not isinstance(resource_id, int):
                HighSeverityWarning(
                    f"Invalid resourceId {resource_id!r} in Canvas {canvas.name}. Skipping."
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
                    f"Failed to look-up {reference_type} external ID for resource ID {resource_id!r}. Skipping resource in Canvas {canvas.name}"
                ).print_warning(console=self.client.console)
                continue
            properties["resourceExternalId"] = external_id
            new_container_references.append(container_ref)
        dumped["containerReferences"] = new_container_references
        return dumped

    def json_chunk_to_data(self, data_chunk: Page[dict[str, JsonVal]]) -> Page[IndustrialCanvasRequest]:
        self._populate_external_id_cache([item.item for item in data_chunk.items])
        return super().json_chunk_to_data(data_chunk)

    @staticmethod
    def _get_properties(container_ref: dict[str, Any]) -> dict[str, Any] | None:
        """Extract properties from a container reference dict, handling both flat and DMS formats."""
        sources = container_ref.get("sources", [])
        if isinstance(sources, list) and len(sources) > 0:
            source = sources[0]
            if isinstance(source, dict) and "properties" in source:
                properties = source["properties"]
                if isinstance(properties, dict):
                    return properties
        if "containerReferenceType" in container_ref:
            return container_ref
        return None

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
                properties = self._get_properties(container_ref)
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

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> IndustrialCanvasRequest:
        return self._load_resource(item_json)

    def _load_resource(self, item_json: dict[str, JsonVal]) -> IndustrialCanvasRequest:
        name = self._get_name(item_json)
        references = item_json.get("containerReferences", [])
        if not isinstance(references, list):
            return IndustrialCanvasRequest._load(item_json)
        new_container_references: list[Any] = []
        for container_ref in references:
            if not isinstance(container_ref, dict):
                new_container_references.append(container_ref)
                continue
            properties = self._get_properties(container_ref)
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
                HighSeverityWarning(
                    f"Failed to look-up {reference_type} ID for external ID {resource_external_id!r}. Skipping resource in Canvas {name}"
                ).print_warning(console=self.client.console)
                continue
            properties["resourceId"] = resource_id
            new_container_references.append(container_ref)
        new_item = dict(item_json)
        new_item["containerReferences"] = new_container_references

        return IndustrialCanvasRequest._load(new_item)

    @classmethod
    def _get_name(cls, item_json: dict[str, JsonVal]) -> str:
        if "name" in item_json:
            name = item_json["name"]
            if isinstance(name, str):
                return name
        try:
            return item_json["canvas"]["sources"][0]["properties"]["name"]  # type: ignore[index,return-value, call-overload]
        except (KeyError, IndexError, TypeError):
            return "<unknown>"
