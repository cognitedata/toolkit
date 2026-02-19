from datetime import datetime
from pathlib import Path

import pytest
from cognite.client.data_classes import DataSet, TimeSeries, TimeSeriesWrite

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands import DownloadCommand, UploadCommand
from cognite_toolkit._cdf_tk.storageio import DatapointsIO
from cognite_toolkit._cdf_tk.storageio.selectors import DataPointsDataSetSelector, DataPointsFileSelector
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

    def test_upload_download_datapoints(
        self, toolkit_client: ToolkitClient, two_timeseries: tuple[TimeSeries, TimeSeries], tmp_path: Path
    ) -> None:
        ts1, _ = two_timeseries
        client = toolkit_client
        assert ts1.is_string is False

        upload_dir = tmp_path / "upload"
        upload_dir.mkdir(parents=True, exist_ok=True)
        selector = DataPointsDataSetSelector(
            data_set_external_id=client.lookup.data_sets.external_id(ts1.data_set_id),
            start=int(datetime.fromisoformat("1989-01-01T00:00:00Z").timestamp() * 1000),
            end=int(datetime.fromisoformat("1989-01-02T00:00:00Z").timestamp() * 1000),
            download_dir_name="datapoints",
        )
        selector.dump_to_file(upload_dir)
        csv_file = upload_dir / f"{selector!s}.{DatapointsIO.KIND}.csv"
        csv_content = """externalId,timestamp,value"""
        rows: list[str] = []
        for i in range(10):
            timestamp_ms = int(datetime.fromisoformat(f"1989-01-01T00:00:{i:02d}Z").timestamp() * 1000)
            rows.append(f"{ts1.external_id},{timestamp_ms},{i * 5}.0")
        csv_content += "\n" + "\n".join(rows)
        csv_file.write_text(csv_content)
        upload_cmd = UploadCommand(silent=True, skip_tracking=True)
        upload_cmd.upload(
            upload_dir,
            toolkit_client,
            deploy_resources=False,
            dry_run=False,
            verbose=True,
        )

        aggregate_result = client.time_series.data.retrieve(
            external_id=ts1.external_id,
            start=datetime.fromisoformat("1989-01-01T00:00:00Z"),
            end=datetime.fromisoformat("1989-01-02T00:00:00Z"),
            aggregates="count",
            granularity="1d",
        )
        assert aggregate_result.count[0] == 10, f"Expected 10 datapoints, got {aggregate_result.count[0]}"

        io = DatapointsIO(client)
        download_cmd = DownloadCommand(silent=True, skip_tracking=True)
        download_cmd.download(
            selectors=[selector],
            io=io,
            output_dir=tmp_path / "download",
            verbose=True,
            file_format=".csv",
            compression="none",
            limit=100_000,
        )

        assert selector.download_dir_name is not None
        download_file = (
            tmp_path / "download" / selector.download_dir_name / f"{selector!s}-part-0000.{DatapointsIO.KIND}.csv"
        )
        assert download_file.exists(), f"Downloaded file {download_file} does not exist"
        actual_output = download_file.read_text(encoding="utf-8-sig")
        assert actual_output.removesuffix("\n") == csv_content, "Downloaded content does not match uploaded content"
