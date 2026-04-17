import json
from datetime import datetime
from typing import Any

import pytest
import responses
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.canvas import (
    CANVAS_INSTANCE_SPACE,
    ContainerReferenceItem,
    IndustrialCanvasResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.chart import ChartResponse
from cognite_toolkit._cdf_tk.client.resource_classes.chart_monitoring_job import (
    ChartMonitoringJobModel,
    ChartMonitoringJobResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.chart_scheduled_calculation import (
    CalculationGraph,
    CalculationInput,
    CalculationStep,
    ChartScheduledCalculationResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.charts_data import (
    ChartData,
    ChartScheduledCalculationUIElement,
    ChartTimeseriesUIElement,
    MonitoringJobReference,
)
from cognite_toolkit._cdf_tk.client.resource_classes.migration import InstanceSource
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.dataio import CanvasIO, ChartIO, DataItem, Page
from cognite_toolkit._cdf_tk.dataio.selectors import (
    AllChartsSelector,
    CanvasExternalIdSelector,
    ChartOwnerSelector,
    ChartSelector,
)

_CHART_TS_EXTERNAL_ID = "shared_ts_ext"
_CHART_TS_INTERNAL_ID = 4242
_CHART_EXTERNAL_ID = "chart_shared_backends"
_CHART_MONITOR_JOB_EXTERNAL_ID = "chart_monitor_job_shared_ts"
_CHART_CALC_EXTERNAL_ID = "chart_scheduled_calc_shared_ts"
_CHART_MONITOR_JOB_INTERNAL_ID = 100
_CHART_MONITOR_JOB_INTERNAL_ID_AFTER_UPLOAD = 999


def _example_scheduled_calculation_response() -> ChartScheduledCalculationResponse:
    graph = CalculationGraph(
        granularity="5m",
        steps=[
            CalculationStep(
                op="PASSTHROUGH",
                version=1.0,
                inputs=[CalculationInput(type="ts", value=_CHART_TS_EXTERNAL_ID)],
                raw=False,
                step=0,
            )
        ],
    )
    return ChartScheduledCalculationResponse(
        external_id=_CHART_CALC_EXTERNAL_ID,
        name="Shared TS scheduled calculation",
        period=300_000,
        window_size=300_000,
        target_timeseries_external_id="shared_ts_calc_output",
        graph=graph,
        created_time=1700000000000,
        last_updated_time=1700000000000,
    )


def _example_monitoring_job_response() -> ChartMonitoringJobResponse:
    return ChartMonitoringJobResponse(
        id=_CHART_MONITOR_JOB_INTERNAL_ID,
        external_id=_CHART_MONITOR_JOB_EXTERNAL_ID,
        name="Shared TS monitoring job",
        channel_id=1,
        model=ChartMonitoringJobModel(
            timeseries_id=_CHART_TS_INTERNAL_ID,
            lower_threshold=0.0,
            upper_threshold=10.0,
        ),
        interval=3600,
        overlap=0,
        source_id=_CHART_EXTERNAL_ID,
    )


def _example_chart_response_for_download() -> ChartResponse:
    monitoring_job = _example_monitoring_job_response()
    calculation = _example_scheduled_calculation_response()
    return ChartResponse(
        external_id=_CHART_EXTERNAL_ID,
        visibility="PUBLIC",
        data=ChartData(
            version=1,
            name="Shared backends chart",
            date_from="2025-01-01T00:00:00.000Z",
            date_to="2025-12-31T23:59:59.999Z",
            time_series_collection=[
                ChartTimeseriesUIElement(ts_id=_CHART_TS_INTERNAL_ID, ts_external_id=None),
            ],
            monitoring_jobs=[
                MonitoringJobReference(id=monitoring_job.id, source_id="src", source_type="chart"),
            ],
            scheduled_calculation_collection=[
                ChartScheduledCalculationUIElement(id=calculation.external_id, name="calc"),
            ],
        ),
        owner_id="99",
        created_time=1700000000000,
        last_updated_time=1700000000000,
    )


@pytest.fixture
def twenty_charts() -> list[ChartResponse]:
    return [
        ChartResponse(
            external_id=f"chart_{i}",
            visibility="PUBLIC",
            data=ChartData(
                version=1, name=f"chart_{i}", date_from="2025-01-01T00:00:00.000Z", date_to="2025-12-31T23:59:59.999Z"
            ),
            owner_id="evenOwner" if i % 2 == 0 else "oddOwner",
            created_time=1700000000000,
            last_updated_time=1700000000000,
        )
        for i in range(20)
    ]


class TestChartIO:
    def test_download_chart(self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter) -> None:
        client = ToolkitClient(config=toolkit_config)
        chart_url = toolkit_config.create_app_url("/storage/charts/charts/list")
        ts_url = toolkit_config.create_api_url("/timeseries/byids")
        selector = AllChartsSelector()
        io = ChartIO(client)

        respx_mock.post(chart_url).respond(
            status_code=200,
            json={
                "items": [
                    {
                        "externalId": "chart_1",
                        "ownerId": "100",
                        "visibility": "PUBLIC",
                        "createdTime": 1700000000000,
                        "lastUpdatedTime": 1700000000000,
                        "data": {
                            "version": 1,
                            "name": "Chart 1",
                            "dateFrom": "2025-01-01T00:00:00.000Z",
                            "dateTo": "2025-12-31T23:59:59.999Z",
                            "timeSeriesCollection": [
                                {"tsId": 200, "tsExternalId": None},
                                {"tsId": None, "tsExternalId": "ts_2"},
                            ],
                        },
                    },
                    {
                        "externalId": "chart_2",
                        "ownerId": "101",
                        "visibility": "PUBLIC",
                        "createdTime": 1700000000000,
                        "lastUpdatedTime": 1700000000000,
                        "data": {
                            "version": 1,
                            "name": "Chart 2",
                            "dateFrom": "2025-01-01T00:00:00.000Z",
                            "dateTo": "2025-12-31T23:59:59.999Z",
                            "timeSeriesCollection": [
                                {"tsId": 201, "tsExternalId": None},
                                {"tsId": None, "tsExternalId": "ts_3"},
                            ],
                        },
                    },
                ]
            },
        )
        with responses.RequestsMock() as rsps:
            rsps.add(
                responses.POST,
                ts_url,
                json={"items": [{"id": 200, "externalId": "ts_1"}, {"id": 201, "externalId": "ts_4"}]},
                status=200,
            )
            assert io.count(selector) == 2
            charts_iterator = io.stream_data(selector=selector)
            json_iterator = (io.data_to_json_chunk(chunk) for chunk in charts_iterator)
            chart_data = [io.json_chunk_to_data(chunk) for chunk in json_iterator]

            assert len(chart_data) == 1
            chart_list = chart_data[0]
            assert len(chart_list) == 2
            first = chart_list.items[0]
            assert first.item.data.time_series_collection[0].ts_external_id == "ts_1"
            assert first.item.data.time_series_collection[1].ts_external_id == "ts_2"
            second = chart_list.items[1]
            assert second.item.data.time_series_collection[0].ts_external_id == "ts_4"
            assert second.item.data.time_series_collection[1].ts_external_id == "ts_3"

    def test_download_chart_with_backend_tasks(self) -> None:
        chart = _example_chart_response_for_download()
        monitoring_job = _example_monitoring_job_response()
        calculation = _example_scheduled_calculation_response()
        with monkeypatch_toolkit_client() as client:
            client.charts.list.return_value = [chart]
            client.charts.scheduled_calculations.retrieve.return_value = [calculation]
            client.charts.monitoring_jobs.list.return_value = [monitoring_job]

            def lookup_ts_ext(arg: object) -> Any:
                if isinstance(arg, list):
                    return [_CHART_TS_EXTERNAL_ID for _ in arg]
                return _CHART_TS_EXTERNAL_ID

            client.lookup.time_series.external_id.side_effect = lookup_ts_ext

            io = ChartIO(client, skip_existing=False, skip_backend_services=False)
            chunk = next(iter(io.stream_data(AllChartsSelector())))
            downloaded_chart = io.data_to_json_chunk(chunk).items[0].item

            client.charts.scheduled_calculations.retrieve.assert_called_once()
            client.charts.monitoring_jobs.list.assert_called_once()

        expected = self._create_downloaded_chart(chart, monitoring_job, calculation)
        assert downloaded_chart == expected

    def _create_downloaded_chart(
        self,
        chart: ChartResponse,
        monitoring_job: ChartMonitoringJobResponse,
        calculation: ChartScheduledCalculationResponse,
    ) -> dict[str, Any]:
        downloaded = chart.as_request_resource().dump()
        downloaded["data"]["timeSeriesCollection"] = [{"tsExternalId": _CHART_TS_EXTERNAL_ID}]
        dumped_job = monitoring_job.as_request_resource().dump()
        dumped_job["id"] = monitoring_job.id
        downloaded["monitoringJobs"] = [dumped_job]
        downloaded["scheduledCalculations"] = [calculation.as_request_resource().dump()]
        return downloaded

    @pytest.mark.usefixtures("disable_gzip")
    def test_upload_chart_with_backend_tasks(self, toolkit_config: ToolkitClientConfig) -> None:
        """Correlate persisted monitoring job internal IDs for upload (see ChartIO._dump_resource job dump)."""
        chart = _example_chart_response_for_download()
        monitoring_job = _example_monitoring_job_response()
        calculation = _example_scheduled_calculation_response()
        downloaded = self._create_downloaded_chart(chart, monitoring_job, calculation)
        new_monitoring_job = monitoring_job.model_copy(update={"id": _CHART_MONITOR_JOB_INTERNAL_ID_AFTER_UPLOAD})
        with monkeypatch_toolkit_client() as client:
            client.charts.list.return_value = []

            def lookup_ts_id(arg: object) -> Any:
                if isinstance(arg, list):
                    return [_CHART_TS_INTERNAL_ID for _ in arg]
                return _CHART_TS_INTERNAL_ID

            client.lookup.time_series.id.side_effect = lookup_ts_id
            client.charts.monitoring_jobs.retrieve.return_value = [new_monitoring_job]
            client.charts.monitoring_jobs.update.return_value = [new_monitoring_job]
            client.charts.scheduled_calculations.retrieve.return_value = [calculation]
            client.charts.scheduled_calculations.update.return_value = [calculation]

            io = ChartIO(client, skip_backend_services=False, skip_existing=False)
            page = io.json_chunk_to_data(
                Page(worker_id="main", items=[DataItem(tracking_id="line 1", item=downloaded)])
            )

            charts_put_url = toolkit_config.create_app_url(ChartIO.UPLOAD_ENDPOINT)
            with HTTPClient(toolkit_config) as http_client:
                with respx.mock(assert_all_called=False) as mock_router:
                    endpoint = mock_router.put(charts_put_url).respond(status_code=200)
                    results = io.upload_items(page, http_client)
                    assert len(endpoint.calls) == 1
                    chart_request = json.loads(endpoint.calls[0].request.content.decode("utf-8"))["items"][0]

            client.charts.monitoring_jobs.update.assert_called_once()
            client.charts.scheduled_calculations.update.assert_called_once()

        assert len(results) == 1
        assert isinstance(results[0], ItemsSuccessResponse)

        assert chart_request["data"]["timeSeriesCollection"][0]["tsExternalId"] == _CHART_TS_EXTERNAL_ID
        assert chart_request["data"]["timeSeriesCollection"][0]["tsId"] == _CHART_TS_INTERNAL_ID
        assert chart_request["data"]["monitoringJobs"][0]["id"] == _CHART_MONITOR_JOB_INTERNAL_ID_AFTER_UPLOAD

    @pytest.mark.parametrize(
        "limit,selector,expected_external_ids",
        [
            pytest.param(None, AllChartsSelector(), [f"chart_{i}" for i in range(20)], id="all charts no limit"),
            pytest.param(5, AllChartsSelector(), [f"chart_{i}" for i in range(5)], id="all charts with limit"),
            pytest.param(
                10,
                AllChartsSelector(),
                [f"chart_{i}" for i in range(10)],
                id="all charts with limit 10 divisible by chunk size",
            ),
            pytest.param(
                None,
                ChartOwnerSelector(owner_id="evenOwner"),
                [f"chart_{i}" for i in range(0, 20, 2)],
                id="even owner no limit",
            ),
            pytest.param(
                3,
                ChartOwnerSelector(owner_id="evenOwner"),
                [f"chart_{i}" for i in range(0, 6, 2)],
                id="even owner with limit",
            ),
        ],
    )
    def test_download_iterable(
        self,
        limit: int | None,
        selector: ChartSelector,
        expected_external_ids: list[str],
        twenty_charts: list[ChartResponse],
    ) -> None:
        with monkeypatch_toolkit_client() as client:
            client.charts.list.return_value = twenty_charts
            io = ChartIO(client)
            chunks = list(io.stream_data(selector=selector, limit=limit))
            all_external_ids: list[str] = []
            for chunk in chunks:
                all_external_ids.extend(di.item.external_id for di in chunk.items)
            assert all_external_ids == expected_external_ids


class TestCanvasIO:
    def test_download_iterable(
        self, asset_centric_canvas: tuple[IndustrialCanvasResponse, list[InstanceSource]]
    ) -> None:
        canvas, _ = asset_centric_canvas
        ids = [
            container_ref.resource_id
            for container_ref in canvas.container_references or []
            if container_ref.container_reference_type not in {"charts", "dataGrid"}
        ]
        assert len(ids) > 0, "Test canvas must have container references for this test to be valid."
        mapping = {id_: f"external_id_{no}" for no, id_ in enumerate(ids)}
        reverse = {v: k for k, v in mapping.items()}

        def reverse_lookup(id_: int | list[int]) -> str | list[str]:
            if isinstance(id_, list):
                return [mapping[i] for i in id_]
            return mapping[id_]

        def lookup(external_id: str | list[str]) -> int | list[int]:
            if isinstance(external_id, list):
                return [reverse[eid] for eid in external_id]
            return reverse[external_id]

        selector = CanvasExternalIdSelector(external_ids=(canvas.external_id,))
        with monkeypatch_toolkit_client() as client:
            client.canvas.retrieve.return_value = [canvas]

            client.lookup.assets.id.side_effect = lookup
            client.lookup.events.id.side_effect = lookup
            client.lookup.time_series.id.side_effect = lookup
            client.lookup.files.id.side_effect = lookup

            client.lookup.assets.external_id.side_effect = reverse_lookup
            client.lookup.events.external_id.side_effect = reverse_lookup
            client.lookup.time_series.external_id.side_effect = reverse_lookup
            client.lookup.files.external_id.side_effect = reverse_lookup

            io = CanvasIO(client)

            chunks = list(io.stream_data(selector=selector))
            assert len(chunks) == 1
            page = chunks[0]
            assert len(page) == 1

            json_page = io.data_to_json_chunk(page, selector)
            assert len(json_page) == 1
            json_format = [di.item for di in json_page.items]
            json_str = json.dumps(json_format)
            internal_id_in_json = [id_ for id_ in ids if str(id_) in json_str]
            assert len(internal_id_in_json) == 0, "Internal IDs should not be present in the JSON export."
            assert "existingVersion" not in json_str, "existingVersion should not be present in the JSON export."
            restored_page = io.json_chunk_to_data(json_page)

            assert len(restored_page) == 1
            restored_canvas = restored_page.items[0]
            assert restored_canvas.item.dump() == canvas.as_request_resource().dump()

    def test_load_canvas_missing_resource(self) -> None:
        with monkeypatch_toolkit_client() as client:
            client.lookup.assets.id.return_value = None
            canvas_json = {
                "externalId": "test_canvas",
                "instanceType": "node",
                "sources": [
                    {
                        "properties": {
                            "createdBy": "doctrino",
                            "name": "Test Canvas",
                            "updatedAt": "2025-12-04T11:40:17.676",
                            "updatedBy": "doctrino",
                        },
                        "source": {
                            "externalId": "Canvas",
                            "space": "cdf_industrial_canvas",
                            "type": "view",
                            "version": "v7",
                        },
                    }
                ],
                "space": "IndustrialCanvasInstanceSpace",
                "containerReferences": [
                    {
                        "externalId": "asset_ref",
                        "instanceType": "node",
                        "sources": [
                            {
                                "properties": {
                                    "containerReferenceType": "asset",
                                    "resourceExternalId": "myAssetId",
                                },
                                "source": {
                                    "externalId": "ContainerReference",
                                    "space": "cdf_industrial_canvas",
                                    "type": "view",
                                    "version": "v2",
                                },
                            }
                        ],
                        "space": "IndustrialCanvasInstanceSpace",
                    }
                ],
            }

            io = CanvasIO(client)
            loaded = io._load_resource(canvas_json)
            assert not loaded.container_references, "Container reference with missing resource should be skipped."

    def test_dump_canvas_missing_resource(self) -> None:
        with monkeypatch_toolkit_client() as client:
            client.lookup.assets.external_id.return_value = None

            canvas = IndustrialCanvasResponse(
                space=CANVAS_INSTANCE_SPACE,
                external_id="test_canvas",
                name="Test Canvas",
                created_by="doctrino",
                updated_by="doctrino",
                updated_at=datetime.now(),
                version=1,
                created_time=123,
                last_updated_time=1,
                container_references=[
                    ContainerReferenceItem(
                        external_id="asset_ref",
                        container_reference_type="asset",
                        resource_id=12345,
                        id_="some-id",
                    ),
                ],
            )

            io = CanvasIO(client)
            dumped = io._dump_resource(canvas)
            container_refs = dumped.get("containerReferences", [])
            assert isinstance(container_refs, list)
            assert len(container_refs) == 0, "Container reference with missing resource should be skipped."
