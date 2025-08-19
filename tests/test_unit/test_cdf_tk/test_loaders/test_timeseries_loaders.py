import pytest
from cognite.client.data_classes import DataPointSubscriptionWrite
from cognite.client.data_classes.data_modeling import NodeId

from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.loaders import DatapointSubscriptionLoader


class TestDatapointSubscriptionLoader:
    @pytest.mark.parametrize(
        "ts_count, node_count, expected_ts_count, expected_node_count, expected_updates",
        [
            pytest.param(150, 150, 100, 0, 2, id="150 timeseries, 150 nodes"),
            pytest.param(20, 0, 20, 0, 0, id="20 timeseries, no nodes"),
            pytest.param(101, 0, 100, 0, 1, id="101 timeseries, no nodes"),
            pytest.param(0, 20, 0, 20, 0, id="no timeseries, 20 nodes"),
            pytest.param(0, 101, 0, 100, 1, id="no timeseries, 101 nodes"),
            pytest.param(50, 150, 50, 50, 1, id="50 timeseries, 150 nodes"),
            pytest.param(150, 50, 100, 0, 1, id="150 timeseries, 50 nodes"),
        ],
    )
    def test_split_timeseries_ids(
        self, ts_count: int, node_count: int, expected_ts_count: int, expected_node_count: int, expected_updates: int
    ) -> None:
        sub = DataPointSubscriptionWrite(
            external_id="mySub",
            partition_count=1,
            time_series_ids=[f"timeseries_{i}" for i in range(ts_count)] or None,
            instance_ids=[NodeId("my_space", f"node_{i}") for i in range(node_count)] or None,
        )

        to_upsert, batches = DatapointSubscriptionLoader.create_split_timeseries_ids(sub)

        assert len(to_upsert.time_series_ids or []) == expected_ts_count
        assert len(to_upsert.instance_ids or []) == expected_node_count
        assert len(batches) == expected_updates

    def test_split_timeseries_ids_raise(self) -> None:
        sub = DataPointSubscriptionWrite(
            external_id="mySub",
            partition_count=1,
            time_series_ids=[f"timeseries_{i}" for i in range(6_000)],
            instance_ids=[NodeId("my_space", f"node_{i}") for i in range(6_000)],
        )

        with pytest.raises(ToolkitValueError) as exc:
            DatapointSubscriptionLoader.create_split_timeseries_ids(sub)

        assert str(exc.value) == 'Subscription "mySub" has 12,000 time series, which is more than the limit of 10,000.'
