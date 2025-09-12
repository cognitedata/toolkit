import pytest
from cognite.client.data_classes import DataPointSubscriptionWrite
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.data_classes.datapoints_subscriptions import TimeSeriesID, TimeSeriesIDList

from cognite_toolkit._cdf_tk.cruds import DatapointSubscriptionCRUD
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError


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
    def test_create_split_timeseries_ids(
        self, ts_count: int, node_count: int, expected_ts_count: int, expected_node_count: int, expected_updates: int
    ) -> None:
        sub = DataPointSubscriptionWrite(
            external_id="mySub",
            partition_count=1,
            time_series_ids=[f"timeseries_{i}" for i in range(ts_count)] or None,
            instance_ids=[NodeId("my_space", f"node_{i}") for i in range(node_count)] or None,
        )

        to_upsert, batches = DatapointSubscriptionCRUD.create_split_timeseries_ids(sub)

        assert len(to_upsert.time_series_ids or []) == expected_ts_count
        assert len(to_upsert.instance_ids or []) == expected_node_count
        assert len(batches) == expected_updates

    def test_create_split_timeseries_ids_raise(self) -> None:
        sub = DataPointSubscriptionWrite(
            external_id="mySub",
            partition_count=1,
            time_series_ids=[f"timeseries_{i}" for i in range(6_000)],
            instance_ids=[NodeId("my_space", f"node_{i}") for i in range(6_000)],
        )

        with pytest.raises(ToolkitValueError) as exc:
            DatapointSubscriptionCRUD.create_split_timeseries_ids(sub)

        assert str(exc.value) == 'Subscription "mySub" has 12,000 time series, which is more than the limit of 10,000.'

    @pytest.mark.parametrize(
        "ts_count, node_count, existing_ts_count, existing_node_count, expected_updates",
        [
            pytest.param(150, 150, 100, 0, 2, id="Adding 50 timeseries and 150 nodes"),
            pytest.param(20, 0, 20, 0, 0, id="No change with existing 20 timeseries"),
            pytest.param(101, 0, 100, 0, 1, id="Adding 1 timeseries to existing 100"),
            pytest.param(0, 20, 0, 20, 0, id="No change with existing nodes"),
            pytest.param(202, 303, 403, 0, 4, id="Removing 101 timeseries and adding 303 nodes"),
            pytest.param(1, 1, 400, 600, 10, id="Removing 399 timeseries and adding 601 nodes"),
        ],
    )
    def test_update_split_timeseries_ids(
        self, ts_count: int, node_count: int, existing_ts_count: int, existing_node_count: int, expected_updates: int
    ) -> None:
        current = TimeSeriesIDList([])
        if existing_ts_count > 0:
            current.extend([TimeSeriesID(external_id=f"timeseries_{i}", id=i) for i in range(existing_ts_count)])
        if existing_node_count > 0:
            current.extend(
                [
                    TimeSeriesID(instance_id=NodeId("my_space", f"node_{i}"), id=i + existing_ts_count)
                    for i in range(existing_node_count)
                ]
            )
        sub = DataPointSubscriptionWrite(
            external_id="mySub",
            partition_count=1,
            time_series_ids=[f"timeseries_{i}" for i in range(ts_count)] or None,
            instance_ids=[NodeId("my_space", f"node_{i}") for i in range(node_count)] or None,
        )

        _, batches = DatapointSubscriptionCRUD.update_split_timeseries_ids(sub, current)
        assert len(batches) == expected_updates

    def test_update_split_timeseries_ids_raise(self) -> None:
        current = TimeSeriesIDList([TimeSeriesID(external_id=f"timeseries_{i}", id=i) for i in range(6_000)])
        sub = DataPointSubscriptionWrite(
            external_id="mySub",
            partition_count=1,
            time_series_ids=[f"timeseries_{i}" for i in range(6_000)],
            instance_ids=[NodeId("my_space", f"node_{i}") for i in range(6_000)],
        )

        with pytest.raises(ToolkitValueError) as exc:
            DatapointSubscriptionCRUD.update_split_timeseries_ids(sub, current)

        assert str(exc.value) == 'Subscription "mySub" has 12,000 time series, which is more than the limit of 10,000.'

    def test_update_split_timeseries_migrated_timeseries(self) -> None:
        current = TimeSeriesIDList(
            [
                TimeSeriesID(id=1, external_id="timeseries_1", instance_id=NodeId("my_space", "node_1")),
                TimeSeriesID(id=1, external_id="timeseries_2", instance_id=NodeId("my_space", "node_2")),
                TimeSeriesID(id=1, external_id="timeseries_3", instance_id=NodeId("my_space", "node_3")),
                TimeSeriesID(id=1, external_id="timeseries_4", instance_id=NodeId("my_space", "node_4")),
            ]
        )
        sub = DataPointSubscriptionWrite(
            external_id="mySub",
            partition_count=1,
            instance_ids=[NodeId("my_space", "node_1"), NodeId("my_space", "node_4")],
            time_series_ids=["timeseries_3", "timeseries_4"],
        )
        _, batches = DatapointSubscriptionCRUD.update_split_timeseries_ids(sub, current)

        assert len(batches) == 1
        assert batches[0].dump() == {
            "externalId": "mySub",
            "update": {
                "instanceIds": {
                    "remove": [{"space": "my_space", "externalId": "node_2"}],
                }
            },
        }
