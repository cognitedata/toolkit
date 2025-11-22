import json

import pytest
import responses
from cognite.client.data_classes.data_modeling import NodeList

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.data_classes.canvas import IndustrialCanvas
from cognite_toolkit._cdf_tk.client.data_classes.charts import Chart, ChartList
from cognite_toolkit._cdf_tk.client.data_classes.charts_data import ChartData
from cognite_toolkit._cdf_tk.client.data_classes.migration import InstanceSource
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.storageio import CanvasIO, ChartIO
from cognite_toolkit._cdf_tk.storageio.selectors import (
    AllChartsSelector,
    CanvasExternalIdSelector,
    ChartOwnerSelector,
    ChartSelector,
)
from tests.test_unit.approval_client import ApprovalToolkitClient


@pytest.fixture
def twenty_charts() -> ChartList:
    return ChartList(
        [
            Chart(
                external_id=f"chart_{i}",
                visibility="PUBLIC",
                data=ChartData(),
                owner_id="evenOwner" if i % 2 == 0 else "oddOwner",
                created_time=1700000000000,
                last_updated_time=1700000000000,
            )
            for i in range(20)
        ]
    )


class TestChartIO:
    def test_download_chart(self, toolkit_config: ToolkitClientConfig) -> None:
        client = ToolkitClient(config=toolkit_config)
        chart_url = toolkit_config.create_app_url("/storage/charts/charts/list")
        ts_url = toolkit_config.create_api_url("/timeseries/byids")
        selector = AllChartsSelector()
        io = ChartIO(client)

        with responses.RequestsMock() as rsps:
            rsps.add(
                responses.POST,
                chart_url,
                json={
                    "items": [
                        {
                            "externalId": "chart_1",
                            "ownerId": 100,
                            "visibility": "PUBLIC",
                            "createdTime": 1700000000000,
                            "lastUpdatedTime": 1700000000000,
                            "data": {
                                "name": "Chart 1",
                                "timeSeriesCollection": [
                                    {"tsId": 200, "tsExternalId": None},
                                    {"tsId": None, "tsExternalId": "ts_2"},
                                ],
                            },
                        },
                        {
                            "externalId": "chart_2",
                            "ownerId": 101,
                            "visibility": "PUBLIC",
                            "createdTime": 1700000000000,
                            "lastUpdatedTime": 1700000000000,
                            "data": {
                                "name": "Chart 2",
                                "timeSeriesCollection": [
                                    {"tsId": 201, "tsExternalId": None},
                                    {"tsId": None, "tsExternalId": "ts_3"},
                                ],
                            },
                        },
                    ]
                },
                status=200,
            )
            rsps.add(
                responses.POST,
                ts_url,
                json={"items": [{"id": 200, "externalId": "ts_1"}, {"id": 201, "externalId": "ts_4"}]},
                status=200,
            )
            assert io.count(selector) is None, (
                "Count should be None since CDF does not provide a way to get the count of charts up front."
            )
            charts_iterator = io.stream_data(selector=selector)
            json_iterator = (io.data_to_json_chunk(chunk.items) for chunk in charts_iterator)
            chart_data = [io.json_chunk_to_data([("id", item) for item in chunk]) for chunk in json_iterator]

            assert len(chart_data) == 1
            chart_list = chart_data[0]
            assert len(chart_list) == 2
            first = chart_list[0]
            assert first.item.data.time_series_collection[0].ts_external_id == "ts_1"
            assert first.item.data.time_series_collection[1].ts_external_id == "ts_2"
            second = chart_list[1]
            assert second.item.data.time_series_collection[0].ts_external_id == "ts_4"
            assert second.item.data.time_series_collection[1].ts_external_id == "ts_3"

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
        self, limit: int | None, selector: ChartSelector, expected_external_ids: list[str], twenty_charts: ChartList
    ) -> None:
        with monkeypatch_toolkit_client() as client:
            client.charts.list.return_value = twenty_charts
            io = ChartIO(client)
            chunks = list(io.stream_data(selector=selector, limit=limit))
            all_charts = ChartList([])
            for chunk in chunks:
                all_charts.extend(chunk.items)
            assert [chart.external_id for chart in all_charts] == expected_external_ids


class TestCanvasIO:
    def test_download_iterable(
        self, asset_centric_canvas: tuple[IndustrialCanvas, NodeList[InstanceSource]], toolkit_client_approval
    ):
        canvas, _ = asset_centric_canvas
        ids = [container_ref.resource_id for container_ref in canvas.container_references or []]
        assert len(ids) > 0, "Test canvas must have container references for this test to be valid."
        selector = CanvasExternalIdSelector(external_ids=(canvas.as_id(),))
        with monkeypatch_toolkit_client() as client:
            approval_client = ApprovalToolkitClient(client, allow_reverse_lookup=True)
            client.canvas.industrial.retrieve.return_value = canvas
            io = CanvasIO(approval_client.mock_client)

            chunks = list(io.stream_data(selector=selector))
            canvas_list = [canvas for page in chunks for canvas in page.items]
            assert len(canvas_list) == 1

            json_format = io.data_to_json_chunk(canvas_list, selector)
            assert len(json_format) == 1
            json_str = json.dumps(json_format[0])  # just to verify it is serializable
            not_removed = [id_ for id_ in ids if str(id_) in json_str]
            assert len(not_removed) == 0, "All internal IDs should be removed from the JSON export."
            restored_canvases = io.json_chunk_to_data([("line 1", item) for item in json_format])

            assert len(restored_canvases) == 1
            restored_canvas = restored_canvases[0]
            assert restored_canvas.item.dump() == canvas.as_write().dump()
