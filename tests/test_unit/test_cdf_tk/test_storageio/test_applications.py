from pathlib import Path

import pytest
import responses

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.data_classes.charts import Chart, ChartList, ChartWriteList
from cognite_toolkit._cdf_tk.client.data_classes.charts_data import ChartData
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.storageio import (
    AllChartSelector,
    ChartFileSelector,
    ChartIO,
    ChartOwnerSelector,
    ChartSelector,
)


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
        selector = AllChartSelector()
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
            json_iterator = (io.data_to_json_chunk(chunk) for chunk in charts_iterator)
            chart_data = [io.json_chunk_to_data(chunk) for chunk in json_iterator]

            assert len(chart_data) == 1
            chart_list = chart_data[0]
            assert isinstance(chart_list, ChartWriteList)
            assert len(chart_list) == 2
            first = chart_list[0]
            assert first.data.time_series_collection[0].ts_external_id == "ts_1"
            assert first.data.time_series_collection[1].ts_external_id == "ts_2"
            second = chart_list[1]
            assert second.data.time_series_collection[0].ts_external_id == "ts_4"
            assert second.data.time_series_collection[1].ts_external_id == "ts_3"

    def test_no_configurations(self) -> None:
        with monkeypatch_toolkit_client() as client:
            io = ChartIO(client)
            selector = AllChartSelector()
            assert list(io.configurations(selector)) == []
            assert io.ensure_configurations(selector, None) is None

    @pytest.mark.parametrize(
        "limit,selector,expected_external_ids",
        [
            pytest.param(None, AllChartSelector(), [f"chart_{i}" for i in range(20)], id="all charts no limit"),
            pytest.param(5, AllChartSelector(), [f"chart_{i}" for i in range(5)], id="all charts with limit"),
            pytest.param(
                10,
                AllChartSelector(),
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
                all_charts.extend(chunk)
            assert [chart.external_id for chart in all_charts] == expected_external_ids

    def test_download_iterable_unsupported_selector(self) -> None:
        with monkeypatch_toolkit_client() as client:
            io = ChartIO(client)

            with pytest.raises(NotImplementedError) as excinfo:
                list(io.stream_data(selector=ChartFileSelector(filepath=Path("some/path.Chart.ndjson"))))

        assert "Unsupported selector type" in str(excinfo.value)
