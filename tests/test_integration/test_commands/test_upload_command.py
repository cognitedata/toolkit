from pathlib import Path

import pytest
from cognite.client.data_classes import DataSet, TimeSeries, TimeSeriesWrite

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands import UploadCommand
from cognite_toolkit._cdf_tk.storageio import DatapointsIO
from cognite_toolkit._cdf_tk.storageio.selectors import DataPointsFileSelector
from cognite_toolkit._cdf_tk.storageio.selectors._datapoints import ExternalIdColumn


@pytest.fixture(scope="session")
def single_timeseries(toolkit_client: ToolkitClient, toolkit_dataset: DataSet) -> TimeSeries:
    ts = TimeSeriesWrite(
        name="Test TimeSeries for Datapoitns IO",
        external_id="test_timeseries_datapoints_io",
        is_step=False,
        is_string=False,
        data_set_id=toolkit_dataset.id,
    )
    if retrieved := toolkit_client.time_series.retrieve(external_id=ts.external_id):
        return retrieved
    return toolkit_client.time_series.create(ts)


class TestDownloadCommand:
    def test_upload_datapoints(
        self, toolkit_client: ToolkitClient, single_timeseries: TimeSeries, tmp_path: Path
    ) -> None:
        csv_file = tmp_path / f"datapoints.{DatapointsIO.KIND}.csv"
        with csv_file.open("w") as f:
            f.write("timestamp,value\n")
            for i in range(10):
                f.write(f"2024-01-01T00:00:{i:02d}Z,{i}\n")
        selector = DataPointsFileSelector(
            path=csv_file.relative_to(tmp_path),
            columns=[
                ExternalIdColumn(
                    column="value",
                    external_id=single_timeseries.external_id,
                )
            ],
        )
        selector.dump_to_file(tmp_path)

        upload_cmd = UploadCommand(silent=True, skip_tracking=True)
        upload_cmd.upload(
            tmp_path,
            toolkit_client,
            deploy_resources=False,
            dry_run=False,
        )

        datapoints = toolkit_client.time_series.data.retrieve_arrays(
            external_id=single_timeseries.external_id,
            start="2024-01-01T00:00:00Z",
            end="2024-01-01T00:00:09Z",
        )
        assert len(datapoints) == 10, f"Expected 10 datapoints, got {len(datapoints)}"
