import pandas as pd
from cognite.client import CogniteClient
from cognite.client.data_classes import TimeSeriesWrite, TimeSeriesWriteList

from cognite_toolkit.cdf_tk.load import TimeSeriesLoader


class TestTimeSeriesLoader:
    def test_create_populate_count_drop_data(self, cognite_client: CogniteClient) -> None:
        timeseries = TimeSeriesWrite(external_id="test_create_populate_count_drop_data", is_string=False)
        datapoints = pd.DataFrame(
            [{"timestamp": 0, timeseries.external_id: 0}, {"timestamp": 1, timeseries.external_id: 1}]
        ).set_index("timestamp")
        datapoints.index = pd.to_datetime(datapoints.index, unit="s")
        loader = TimeSeriesLoader(client=cognite_client)
        ts_ids = [timeseries.external_id]

        try:
            created = loader.create(TimeSeriesWriteList([timeseries]))
            assert len(created) == 1

            assert loader.count(ts_ids) == 0
            cognite_client.time_series.data.insert_dataframe(datapoints)

            assert loader.count(ts_ids) == 2

            loader.drop_data(ts_ids)

            assert loader.count(ts_ids) == 0

            assert loader.delete(ts_ids) == 1

            assert not loader.retrieve(ts_ids)
        finally:
            cognite_client.time_series.delete(external_id=timeseries.external_id, ignore_unknown_ids=True)
