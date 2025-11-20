from datetime import datetime
from pathlib import Path

import pytest
from cognite.client.data_classes import DataSet, TimeSeries, TimeSeriesWrite

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands import UploadCommand
from cognite_toolkit._cdf_tk.storageio import DatapointsIO
from cognite_toolkit._cdf_tk.storageio.selectors import DataPointsFileSelector
from cognite_toolkit._cdf_tk.storageio.selectors._datapoints import ExternalIdColumn, InternalIdColumn


@pytest.fixture(scope="session")
def two_timeseries(toolkit_client: ToolkitClient, toolkit_dataset: DataSet) -> tuple[TimeSeries, TimeSeries]:
    ts = TimeSeriesWrite(
        name="Test TimeSeries for Datapoints IO",
        external_id="test_timeseries_datapoints_io",
        is_step=False,
        is_string=False,
        data_set_id=toolkit_dataset.id,
    )
    ts2 = TimeSeriesWrite(
        name="Test TimeSeries 2 for Datapoints IO",
        external_id="test_timeseries_2_datapoints_io",
        is_step=False,
        is_string=True,
        data_set_id=toolkit_dataset.id,
    )
    retrieved1 = toolkit_client.time_series.retrieve(external_id=ts.external_id)
    retrieved2 = toolkit_client.time_series.retrieve(external_id=ts2.external_id)
    if retrieved1 is None:
        retrieved1 = toolkit_client.time_series.create(ts)
    if retrieved2 is None:
        retrieved2 = toolkit_client.time_series.create(ts2)
    return retrieved1, retrieved2


class TestUploadCommand:
    def test_upload_datapoints(
        self, toolkit_client: ToolkitClient, two_timeseries: tuple[TimeSeries, TimeSeries], tmp_path: Path
    ) -> None:
        ts1, ts2 = two_timeseries
        assert ts1.is_string is False
        assert ts2.is_string is True
        selector = DataPointsFileSelector(
            timestamp_column="timestamp",
            columns=(
                ExternalIdColumn(
                    column="value",
                    external_id=ts1.external_id,
                    dtype="numeric",
                ),
                InternalIdColumn(
                    column="value2",
                    internal_id=ts2.id,
                    dtype="string",
                ),
            ),
        )
        selector.dump_to_file(tmp_path)
        csv_file = tmp_path / f"{selector!s}.{DatapointsIO.KIND}.csv"

        with csv_file.open("w") as f:
            f.write("timestamp,value,value2\n")
            for i in range(10):
                f.write(f"2024-01-01T00:00:{i:02d}Z,{i},no_{i * 10}\n")

        upload_cmd = UploadCommand(silent=True, skip_tracking=True)
        upload_cmd.upload(
            tmp_path,
            toolkit_client,
            deploy_resources=False,
            dry_run=False,
            verbose=True,
        )

        datapoints = toolkit_client.time_series.data.retrieve_arrays(
            external_id=ts1.external_id,
            start=datetime.fromisoformat("2024-01-01T00:00:00Z"),
            end=datetime.fromisoformat("2024-01-01T00:00:10Z"),
        )
        assert len(datapoints) == 10, f"Expected 10 datapoints, got {len(datapoints)}"
        datapoints2 = toolkit_client.time_series.data.retrieve_arrays(
            external_id=ts2.external_id,
            start=datetime.fromisoformat("2024-01-01T00:00:00Z"),
            end=datetime.fromisoformat("2024-01-01T00:00:10Z"),
        )
        assert len(datapoints2) == 10, f"Expected 10 datapoints, got {len(datapoints2)}"
