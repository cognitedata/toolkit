from pathlib import Path

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands import DownloadCommand
from cognite_toolkit._cdf_tk.constants import DATA_MANIFEST_STEM
from cognite_toolkit._cdf_tk.storageio import RawIO
from cognite_toolkit._cdf_tk.storageio.selectors import RawTableSelector, SelectedTable
from cognite_toolkit._cdf_tk.utils.file import read_yaml_file
from cognite_toolkit._cdf_tk.utils.fileio import NDJsonReader
from tests.test_integration.constants import TIMESERIES_COUNT, TIMESERIES_TABLE


class TestDownloadCommand:
    def test_download_raw_table(self, toolkit_client: ToolkitClient, aggregator_raw_db: str, tmp_path: Path) -> None:
        cmd = DownloadCommand(silent=True, skip_tracking=True)
        table = RawTableSelector(table=SelectedTable(db_name=aggregator_raw_db, table_name=TIMESERIES_TABLE))
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
        config_files = [file for file in tmp_path.rglob("*.yaml") if not file.stem.endswith(DATA_MANIFEST_STEM)]
        assert len(config_files) == 2, "Expected two configuration file to be created."
        table_file = next((file for file in config_files if file.stem.endswith("Table")), None)
        assert table_file is not None, "Table configuration file not found."
        dumped = read_yaml_file(table_file, "dict")
        assert dumped == table.dump()["table"]
        database_file = next((file for file in config_files if file.stem.endswith("Database")), None)
        assert database_file is not None, "Database configuration file not found."
        dumped_db = read_yaml_file(database_file, "dict")
        assert dumped_db == {"db_name": table.table.db_name}
