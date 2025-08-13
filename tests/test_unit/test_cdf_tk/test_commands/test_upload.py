from pathlib import Path

from cognite.client.data_classes.raw import RowWrite

from cognite_toolkit._cdf_tk.client.data_classes.raw import RawTable
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands import UploadCommand
from cognite_toolkit._cdf_tk.loaders import RawTableLoader
from cognite_toolkit._cdf_tk.storageio import RawIO
from cognite_toolkit._cdf_tk.utils.fileio import NDJsonWriter, NoneCompression


class TestUploadCommand:
    def test_upload_raw_rows(self, tmp_path: Path) -> None:
        folder = tmp_path / RawIO.folder_name
        folder.mkdir(parents=True, exist_ok=True)
        configfile = folder / f"test_table.{RawTableLoader.kind}.yaml"
        table = RawTable(db_name="test_db", table_name="test_table")
        configfile.write_text(table.dump_yaml())
        with NDJsonWriter(folder, RawIO.kind, NoneCompression) as writer:
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

        cmd = UploadCommand(silent=True, skip_tracking=True)
        with monkeypatch_toolkit_client() as client:
            cmd.upload(RawIO(client), folder, verbose=False)

            client.raw.rows.insert.assert_called_once()
            _, kwargs = client.raw.rows.insert.call_args
            assert kwargs["db_name"] == "test_db"
            assert kwargs["table_name"] == "test_table"
            inserted_rows = kwargs["row"]
            assert len(inserted_rows) == 1_000
            assert all(isinstance(row, RowWrite) for row in inserted_rows)
