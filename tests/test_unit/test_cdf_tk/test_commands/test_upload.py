import json
from pathlib import Path

import pytest
import respx
from cognite.client.data_classes.raw import RowWrite

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
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
    @pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
    def test_upload_raw_rows(
        self, toolkit_config: ToolkitClientConfig, raw_directory: Path, tmp_path: Path, respx_mock: respx.MockRouter
    ) -> None:
        config = toolkit_config
        insert_url = config.create_api_url("/raw/dbs/test_db/tables/test_table/rows")
        respx_mock.post(insert_url).respond(status_code=200)

        cmd = UploadCommand(silent=True, skip_tracking=True)
        with monkeypatch_toolkit_client() as client:
            client.config = config
            cmd.upload(RawIO(client), raw_directory, ensure_configurations=True, dry_run=False, verbose=False)

            assert len(respx_mock.calls) == 1
            call = respx_mock.calls[0]
            assert call.request.url == insert_url
            body = json.loads(call.request.content)["items"]
            assert isinstance(body, list)
            assert len(body) == 1_000

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
