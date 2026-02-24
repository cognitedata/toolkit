import json
from datetime import datetime

import pytest
import responses
import respx
from cognite.client.data_classes.data_modeling import NodeList, NodeListWithCursor

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.resource_classes.charts import ChartResponse
from cognite_toolkit._cdf_tk.client.resource_classes.charts_data import ChartData
from cognite_toolkit._cdf_tk.client.resource_classes.legacy.canvas import (
    Canvas,
    ContainerReference,
    IndustrialCanvas,
)
from cognite_toolkit._cdf_tk.client.resource_classes.legacy.migration import InstanceSource
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.storageio import CanvasIO, ChartIO
from cognite_toolkit._cdf_tk.storageio.selectors import (
    AllChartsSelector,
    CanvasExternalIdSelector,
    ChartOwnerSelector,
    ChartSelector,
)


@pytest.fixture
def twenty_charts() -> list[ChartResponse]:
    return [
        ChartResponse(
            external_id=f"chart_{i}",
            visibility="PUBLIC",
            data=ChartData(),
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
                            "name": "Chart 1",
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
                            "name": "Chart 2",
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
            all_charts: list[ChartResponse] = []
            for chunk in chunks:
                all_charts.extend(chunk.items)
            assert [chart.external_id for chart in all_charts] == expected_external_ids


class TestCanvasIO:
    def test_download_iterable(self, asset_centric_canvas: tuple[IndustrialCanvas, NodeList[InstanceSource]]) -> None:
        canvas, _ = asset_centric_canvas
        # Exclude charts/dataGrid types which have resourceId=-1 by design
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

        selector = CanvasExternalIdSelector(external_ids=(canvas.as_id(),))
        with monkeypatch_toolkit_client() as client:
            client.canvas.industrial.retrieve.return_value = canvas

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
            canvas_list = [canvas for page in chunks for canvas in page.items]
            assert len(canvas_list) == 1

            json_format = io.data_to_json_chunk(canvas_list, selector)
            assert len(json_format) == 1
            json_str = json.dumps(json_format)
            internal_id_in_json = [id_ for id_ in ids if str(id_) in json_str]
            assert len(internal_id_in_json) == 0, "Internal IDs should not be present in the JSON export."
            assert "existingVersion" not in json_str, "existingVersion should not be present in the JSON export."
            restored_canvases = io.json_chunk_to_data([("line 1", item) for item in json_format])

            assert len(restored_canvases) == 1
            restored_canvas = restored_canvases[0]
            # The restored canvas should match the original without existingVersion fields
            assert restored_canvas.item.dump() == canvas.as_write().dump(keep_existing_version=False)

    def test_load_canvas_missing_resource(self) -> None:
        with monkeypatch_toolkit_client() as client:
            client.lookup.assets.id.return_value = None
            canvas_json = {
                "canvas": {
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
                },
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
            assert len(loaded.container_references) == 0, "Container reference with missing resource should be skipped."

    def test_dump_canvas_missing_resource(self) -> None:
        with monkeypatch_toolkit_client() as client:
            client.lookup.assets.external_id.return_value = None

            canvas = IndustrialCanvas(
                canvas=Canvas(
                    "my_space",
                    "test_canvas",
                    version=1,
                    last_updated_time=1,
                    created_by=1,
                    name="Test Canvas",
                    updated_by="doctrino",
                    updated_at=datetime.now(),
                    created_time=123,
                ),
                container_references=NodeListWithCursor[ContainerReference](
                    [
                        ContainerReference(
                            external_id="asset_ref",
                            container_reference_type="asset",
                            resource_id=12345,
                            space="my_space",
                            version=1,
                            last_updated_time=1,
                            created_time=1,
                        ),
                    ],
                    cursor=None,
                ),
            )

            io = CanvasIO(client)
            dumped = io._dump_resource(canvas)
            assert len(dumped["containerReferences"]) == 0, (
                "Container reference with missing resource should be skipped."
            )
