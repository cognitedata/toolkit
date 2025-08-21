from pathlib import Path

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.raw import RawTable
from cognite_toolkit._cdf_tk.commands import DownloadCommand
from cognite_toolkit._cdf_tk.storageio import RawIO
from cognite_toolkit._cdf_tk.utils.file import read_yaml_file
from cognite_toolkit._cdf_tk.utils.fileio import NDJsonReader
from tests.test_integration.constants import TIMESERIES_COUNT, TIMESERIES_TABLE


class TestDownloadCommand:
    def test_download_raw_table(self, toolkit_client: ToolkitClient, aggregator_raw_db: str, tmp_path: Path) -> None:
        cmd = DownloadCommand(silent=True, skip_tracking=True)
        table = RawTable(db_name=aggregator_raw_db, table_name=TIMESERIES_TABLE)
        cmd.download(
            [table],
            RawIO(toolkit_client),
            output_dir=tmp_path,
            verbose=False,
            file_format=".ndjson",
            compression="none",
            limit=None,
        )
        downloaded_files = list(tmp_path.rglob("*.ndjson"))
        assert len(downloaded_files) == 1, "Expected exactly one file to be downloaded."
        chunks = list(NDJsonReader(downloaded_files[0]).read_chunks())
        assert len(chunks) == TIMESERIES_COUNT, f"Expected {TIMESERIES_COUNT} chunks, got {len(chunks)}."
        config_files = list(tmp_path.rglob("*.yaml"))
        assert len(config_files) == 1, "Expected exactly one configuration file to be created."
        dumped = read_yaml_file(config_files[0], "dict")
        assert dumped == table.dump()
