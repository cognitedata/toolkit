from datetime import datetime

from cognite.client.data_classes import TimeSeries, TimeSeriesWrite
from cognite.client.data_classes.data_modeling import NodeApplyResultList
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteTimeSeriesApply
from cognite.client.utils._time import datetime_to_ms

from cognite_toolkit._cdf_tk.client import ToolkitClient


class TestExtendedTimeSeriesAPI:
    def test_set_pending_instance_id(self, dev_cluster_client: ToolkitClient, dev_space: str) -> None:
        """Happy path for setting a pending instance ID on a time series.

        1. Create asset-centric time series.
        2. Insert data points.
        3. Set pending instance ID.
        4. Create a CogniteTimeSeries
        5. Retrieve data points using the Node ID.
        6. Insert more data points using the Node ID.
        7. Retrieve data points again using the external ID of the original asset-centric time series.
        """
        client = dev_cluster_client
        ts = TimeSeriesWrite(
            external_id="ts_toolkit_integration_test_happy_path",
            name="Toolkit Integration Test Happy Path",
            is_step=False,
            is_string=False,
        )
        cognite_ts = CogniteTimeSeriesApply(
            space=dev_space,
            external_id=ts.external_id,
            is_step=False,
            time_series_type="numeric",
            name="Toolkit Integration Test Happy Path",
        )
        datapoints = [
            {"timestamp": datetime_to_ms(datetime(2020, 1, 1, 0, 0, 0)), "value": 1.0},
            {"timestamp": datetime_to_ms(datetime(2020, 1, 2, 0, 0, 0)), "value": 2.0},
            {"timestamp": datetime_to_ms(datetime(2020, 1, 3, 0, 0, 0)), "value": 3.0},
        ]
        more_datapoints = [
            {"timestamp": datetime_to_ms(datetime(2020, 1, 4, 0, 0, 0)), "value": 4.0},
            {"timestamp": datetime_to_ms(datetime(2020, 1, 5, 0, 0, 0)), "value": 5.0},
        ]
        created: TimeSeries | None = None
        created_dm: NodeApplyResultList | None = None
        try:
            created = client.time_series.create(ts)
            client.time_series.data.insert(datapoints, external_id=ts.external_id)
            updated = client.time_series.set_pending_ids(cognite_ts.as_id(), external_id=ts.external_id)
            assert updated.pending_instance_id == cognite_ts.as_id()

            created_dm = client.data_modeling.instances.apply(cognite_ts).nodes

            retrieve_datapoints = client.time_series.data.retrieve(instance_id=cognite_ts.as_id())
            assert len(retrieve_datapoints) == len(datapoints)
            assert retrieve_datapoints.dump()["datapoints"] == datapoints
            client.time_series.data.insert(more_datapoints, instance_id=cognite_ts.as_id())

            retrieve_datapoints2 = client.time_series.data.retrieve(external_id=ts.external_id)
            assert len(retrieve_datapoints2) == len(datapoints) + len(more_datapoints)
            assert retrieve_datapoints2.dump()["datapoints"] == datapoints + more_datapoints

            retrieved_ts = client.time_series.retrieve(external_id=ts.external_id)
            assert retrieved_ts.instance_id == cognite_ts.as_id()
            listed = client.time_series.list(external_id_prefix=ts.external_id)
            assert len(listed) == 1
            assert listed[0].external_id == ts.external_id
        finally:
            if created is not None and created_dm is None:
                client.time_series.delete(external_id=ts.external_id)
            if created_dm is not None:
                # This will delete the CogniteTimeSeries and the asset-centric time series
                client.data_modeling.instances.delete(cognite_ts.as_id())

    def test_unlink_instance_ids(self, dev_cluster_client: ToolkitClient, space: str) -> None:
        client = dev_cluster_client
        ts = TimeSeriesWrite(
            external_id="ts_toolkit_integration_test_unlink",
            name="Toolkit Integration Test Unlink",
            is_step=False,
            is_string=False,
        )
        cognite_ts = CogniteTimeSeriesApply(
            space=space,
            external_id=ts.external_id,
            is_step=False,
            time_series_type="numeric",
            name="Toolkit Integration Test Unlink",
        )
        created: TimeSeries | None = None
        created_dm: NodeApplyResultList | None = None
        try:
            created = client.time_series.create(ts)
            updated = client.time_series.set_pending_ids(cognite_ts.as_id(), external_id=ts.external_id)
            assert updated.pending_instance_id == cognite_ts.as_id()

            created_dm = client.data_modeling.instances.apply(cognite_ts).nodes

            retrieved_ts = client.time_series.retrieve(instance_id=cognite_ts.as_id())
            assert retrieved_ts.id == created.id

            unlinked = client.time_series.unlink_instance_ids(id=created.id)
            assert unlinked.id == created.id

            client.data_modeling.instances.delete(cognite_ts.as_id())
            created_dm = None

            # Still existing time series.
            retrieved_ts = client.time_series.retrieve(external_id=ts.external_id)
            assert retrieved_ts is not None
            assert retrieved_ts.id == created.id
        finally:
            if created is not None and created_dm is None:
                client.time_series.delete(external_id=ts.external_id)
            if created_dm is not None:
                client.data_modeling.instances.delete(cognite_ts.as_id())
