import csv
import random
from datetime import datetime
from pathlib import Path

import pytest
from cognite.client.data_classes import TimeSeriesList, TimeSeriesWrite, TimeSeriesWriteList
from cognite.client.data_classes.data_modeling import NodeId, Space
from cognite.client.exceptions import CogniteAPIError

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.extended_timeseries import ExtendedTimeSeriesList
from cognite_toolkit._cdf_tk.commands import MigrateTimeseriesCommand


@pytest.fixture()
def three_timeseries_with_datapoints(
    toolkit_client_with_pending_ids: ToolkitClient, toolkit_space: Space
) -> TimeSeriesList:
    client = toolkit_client_with_pending_ids
    space = toolkit_space.space
    timeseries = TimeSeriesWriteList([])
    for i in range(3):
        ts = TimeSeriesWrite(
            external_id=f"toolkit_test_migration_{i}_{random.randint(0, 10_000)!s}",
            name=f"toolkit_test_migration_{i}",
            is_string=False,
            is_step=False,
        )
        timeseries.append(ts)
    output = client.time_series.retrieve_multiple(external_ids=timeseries.as_external_ids(), ignore_unknown_ids=True)
    if output:
        try:
            client.time_series.delete(external_id=output.as_external_ids(), ignore_unknown_ids=True)
        except CogniteAPIError:
            client.data_modeling.instances.delete([NodeId(space, ts.external_id) for ts in output])
    created = client.time_series.create(timeseries)
    client.time_series.data.insert_multiple(
        [
            dict(
                external_id=ts.external_id,
                datapoints=[(datetime(2020, 1, 1, 0, 0, 0), 1.0)],
            )
            for ts in created
        ]
    )

    yield created

    # Cleanup after test
    deleted = client.data_modeling.instances.delete([NodeId(space, ts.external_id) for ts in created])
    if deleted.nodes:
        return
    client.time_series.delete(external_id=created.as_external_ids())


class TestMigrateTimeSeriesCommand:
    # This tests uses instances.apply_fast() which uses up to 4 workers for writing instances,
    # when this is used in parallel with other tests that uses instances.apply() then we get 5 workers in total,
    # which will trigger a 429 error.
    @pytest.mark.usefixtures("max_two_workers")
    def test_migrate_timeseries_command(
        self,
        toolkit_client_with_pending_ids: ToolkitClient,
        three_timeseries_with_datapoints: ExtendedTimeSeriesList,
        toolkit_space: Space,
        tmp_path: Path,
    ) -> None:
        client = toolkit_client_with_pending_ids
        space = toolkit_space.space

        input_file = tmp_path / "timeseries_migration.csv"
        with input_file.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "dataSetId", "space", "externalId"])
            for ts in three_timeseries_with_datapoints:
                writer.writerow([ts.id, ts.data_set_id, space, ts.external_id])

        cmd = MigrateTimeseriesCommand(skip_tracking=True, silent=True)
        cmd.migrate_timeseries(
            client=client,
            mapping_file=input_file,
            dry_run=False,
            verbose=False,
            auto_yes=True,
        )

        instance_ids = [NodeId(space, x.external_id) for x in three_timeseries_with_datapoints]
        migrated_timeseries = client.time_series.retrieve_multiple(instance_ids=instance_ids)

        # Assert that the CogniteTimeSeries are the same time series as the one we tried to migrate.
        assert set(x.id for x in three_timeseries_with_datapoints) == set(x.id for x in migrated_timeseries)

        # Check that we still have data points
        datapoints = client.time_series.data.retrieve_latest(instance_id=instance_ids)
        assert len(datapoints) == len(migrated_timeseries)
        missing_datapoints = [dp for dp in datapoints if len(dp.value) == 0]
        assert not missing_datapoints, f"Some timeseries are missing data points: {missing_datapoints}"
