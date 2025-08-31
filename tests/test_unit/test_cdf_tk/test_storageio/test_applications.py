import responses

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.data_classes.charts import ChartWriteList
from cognite_toolkit._cdf_tk.storageio import AllChartSelector, ChartIO


class TestChartIO:
    def test_download_chart(self, toolkit_config: ToolkitClientConfig) -> None:
        client = ToolkitClient(config=toolkit_config)
        url = toolkit_config.create_app_url("/storage/charts/charts/list")
        selector = AllChartSelector()
        io = ChartIO(client)

        with responses.RequestsMock() as rsps:
            rsps.add(
                responses.POST,
                url,
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
                toolkit_config.create_api_url("/timeseries/byids"),
                json={"items": [{"id": 200, "externalId": "ts_1"}]},
                status=200,
            )
            rsps.add(
                responses.POST,
                toolkit_config.create_api_url("/timeseries/byids"),
                json={"items": [{"id": 201, "externalId": "ts_4"}]},
                status=200,
            )
            charts_iterator = io.download_iterable(selector=selector)
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
