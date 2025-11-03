import json
from collections.abc import Iterator
from pathlib import Path

import pytest
import respx
from cognite.client.data_classes.raw import RowWrite, Table, TableList

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.data_classes.raw import RawTable, RawTableList
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands import UploadCommand
from cognite_toolkit._cdf_tk.constants import DATA_RESOURCE_DIR
from cognite_toolkit._cdf_tk.cruds import RawTableCRUD
from cognite_toolkit._cdf_tk.storageio import RawIO
from cognite_toolkit._cdf_tk.storageio.selectors import RawTableSelector, SelectedTable
from cognite_toolkit._cdf_tk.utils.fileio import NDJsonWriter, Uncompressed


@pytest.fixture
def raw_json_directory(tmp_path: Path) -> Path:
    """Fixture to create a temporary folder with a sample NDJSON file."""
    configfile = tmp_path / DATA_RESOURCE_DIR / RawTableCRUD.folder_name / f"test_table.{RawTableCRUD.kind}.yaml"
    configfile.parent.mkdir(parents=True, exist_ok=True)
    table = RawTable(db_name="test_db", table_name="test_table")
    configfile.write_text(table.dump_yaml())
    with NDJsonWriter(tmp_path, RawIO.KIND, Uncompressed) as writer:
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

    selector = RawTableSelector(
        table=SelectedTable(db_name=table.db_name, table_name=table.table_name), type="rawTable"
    )
    selector.dump_to_file(tmp_path)
    return tmp_path


@pytest.fixture
def raw_csv_directory(tmp_path: Path) -> Path:
    """Fixture to create a temporary folder with a sample CSV file."""
    configfile = tmp_path / DATA_RESOURCE_DIR / RawTableCRUD.folder_name / f"test_table.{RawTableCRUD.kind}.yaml"
    configfile.parent.mkdir(parents=True, exist_ok=True)
    table = RawTable(db_name="test_db", table_name="test_table")
    configfile.write_text(table.dump_yaml())
    csv_file = tmp_path / f"test_table.{RawIO.KIND}.csv"
    with csv_file.open("w") as f:
        f.write("index,column1,column2,column3\n")
        for i in range(1, 1001):
            f.write(f"key{i},value{i},{i},{i % 2 == 0}\n")

    selector = RawTableSelector(
        table=SelectedTable(db_name=table.db_name, table_name=table.table_name), type="rawTable", key="index"
    )
    selector.dump_to_file(tmp_path)
    return tmp_path


@pytest.fixture
def raw_mock_client(
    toolkit_config: ToolkitClientConfig,
    respx_mock: respx.MockRouter,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[tuple[ToolkitClient, respx.MockRouter]]:
    config = toolkit_config
    insert_url = config.create_api_url("/raw/dbs/test_db/tables/test_table/rows")
    respx_mock.post(url=insert_url).respond(status_code=200)
    monkeypatch.setenv("CDF_CLUSTER", config.cdf_cluster)
    monkeypatch.setenv("CDF_PROJECT", config.project)
    with monkeypatch_toolkit_client() as client:
        client.verify.authorization.return_value = []
        client.raw.tables.list.return_value = RawTableList([])
        client.raw.tables.create.return_value = TableList([Table(name="test_table")])
        client.config = config
        yield client, respx_mock


class TestUploadCommand:
    @pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
    def test_upload_raw_rows_from_ndjson(
        self,
        raw_mock_client: tuple[ToolkitClient, respx.MockRouter],
        raw_json_directory: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        client, respx_mock = raw_mock_client

        cmd = UploadCommand(silent=True, skip_tracking=True)
        cmd.upload(raw_json_directory, client, deploy_resources=True, dry_run=False, verbose=False)

        self.assert_raw_rows_uploaded(client, respx_mock)

    @pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
    def test_upload_raw_rows_from_csv(
        self,
        raw_mock_client: tuple[ToolkitClient, respx.MockRouter],
        raw_csv_directory: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        client, respx_mock = raw_mock_client

        cmd = UploadCommand(silent=True, skip_tracking=True)
        cmd.upload(raw_csv_directory, client, deploy_resources=True, dry_run=False, verbose=False)

        self.assert_raw_rows_uploaded(client, respx_mock)

    def assert_raw_rows_uploaded(self, client: ToolkitClient, respx_mock: respx.MockRouter) -> None:
        assert len(respx_mock.calls) == 1
        call = respx_mock.calls[0]
        assert str(call.request.url).endswith("/raw/dbs/test_db/tables/test_table/rows")
        body = json.loads(call.request.content)["items"]
        assert isinstance(body, list)
        assert len(body) == 1_000

        client.raw.tables.create.assert_called_once()
        _, table_kwargs = client.raw.tables.create.call_args
        assert table_kwargs["db_name"] == "test_db"
        assert table_kwargs["name"] == ["test_table"]

    def test_upload_raw_rows_dry_run(
        self, toolkit_config: ToolkitClientConfig, raw_json_directory: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        config = toolkit_config
        cmd = UploadCommand(silent=True, skip_tracking=True)
        monkeypatch.setenv("CDF_CLUSTER", config.cdf_cluster)
        monkeypatch.setenv("CDF_PROJECT", config.project)
        with monkeypatch_toolkit_client() as client:
            client.verify.authorization.return_value = []
            cmd.upload(raw_json_directory, client, deploy_resources=True, dry_run=True, verbose=False)

            client.raw.rows.insert.assert_not_called()
            client.raw.databases.create.assert_not_called()
            client.raw.tables.create.assert_not_called()
