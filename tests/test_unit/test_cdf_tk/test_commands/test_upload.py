from pathlib import Path

import pytest
from cognite.client.data_classes.raw import RowWrite

from cognite_toolkit._cdf_tk.client.data_classes.raw import RawTable
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands import UploadCommand
from cognite_toolkit._cdf_tk.cruds import RawTableCRUD
from cognite_toolkit._cdf_tk.storageio import RawIO
from cognite_toolkit._cdf_tk.utils.fileio import NDJsonWriter, Uncompressed


@pytest.fixture
def raw_directory(tmp_path: Path) -> Path:
    """Fixture to create a temporary folder with a sample NDJSON file."""
    folder = tmp_path / RawIO.FOLDER_NAME
    folder.mkdir(parents=True, exist_ok=True)
    configfile = folder / f"test_table.{RawTableCRUD.kind}.yaml"
    table = RawTable(db_name="test_db", table_name="test_table")
    configfile.write_text(table.dump_yaml())
    with NDJsonWriter(folder, RawIO.KIND, Uncompressed) as writer:
        writer.write_chunks(
            [
                RowWrite(
                    key=f"key{i}",
                    columns={
                        "column1": f"value{i}",
                        "column2": i,
                        "column3": i % 2 == 0,
                    },
                ).dump()
                for i in range(1_000)
            ],
            filestem="test_table",
        )

    return folder


class TestUploadCommand:
    def test_upload_raw_rows(self, raw_directory: Path, tmp_path: Path) -> None:
        cmd = UploadCommand(silent=True, skip_tracking=True)
        with monkeypatch_toolkit_client() as client:
            cmd.upload(RawIO(client), raw_directory, ensure_configurations=True, dry_run=False, verbose=False)

            client.raw.rows.insert.assert_called_once()
            _, kwargs = client.raw.rows.insert.call_args
            assert kwargs["db_name"] == "test_db"
            assert kwargs["table_name"] == "test_table"
            inserted_rows = kwargs["row"]
            assert len(inserted_rows) == 1_000
            assert all(isinstance(row, RowWrite) for row in inserted_rows)

            client.raw.databases.create.assert_called_once()
            db_args, _ = client.raw.databases.create.call_args
            assert db_args[0] == ["test_db"]
            client.raw.tables.create.assert_called_once()
            _, table_kwargs = client.raw.tables.create.call_args
            assert table_kwargs["db_name"] == "test_db"
            assert table_kwargs["name"] == ["test_table"]

    def test_upload_raw_rows_dry_run(self, raw_directory: Path) -> None:
        cmd = UploadCommand(silent=True, skip_tracking=True)
        with monkeypatch_toolkit_client() as client:
            cmd.upload(RawIO(client), raw_directory, ensure_configurations=True, dry_run=True, verbose=False)

            client.raw.rows.insert.assert_not_called()
            client.raw.databases.create.assert_not_called()
            client.raw.tables.create.assert_not_called()
