import json
from collections.abc import Iterator
from pathlib import Path

import pytest
import respx
from cognite.client.data_classes import data_modeling as dm
from cognite.client.data_classes.raw import RowWrite, Table, TableList

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.data_classes.raw import RawTable, RawTableList
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands import UploadCommand
from cognite_toolkit._cdf_tk.constants import DATA_RESOURCE_DIR
from cognite_toolkit._cdf_tk.cruds import RawTableCRUD
from cognite_toolkit._cdf_tk.storageio import RawIO
from cognite_toolkit._cdf_tk.storageio.selectors import (
    InstanceSpaceSelector,
    RawTableSelector,
    SelectedTable,
    SelectedView,
    Selector,
)
from cognite_toolkit._cdf_tk.utils.fileio import NDJsonWriter, Uncompressed
from tests.test_unit.approval_client import ApprovalToolkitClient


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

    def test_prepare_sorts_instance_selectors_by_dependencies(
        self, toolkit_client_approval: ApprovalToolkitClient
    ) -> None:
        """Test that _prepare sorts instance selectors based on container dependencies."""
        cmd = UploadCommand(silent=True, skip_tracking=True)

        # Create containers with dependencies
        container_a = dm.Container(
            space="my_space",
            external_id="ContainerA",
            properties={
                "name": dm.ContainerProperty(type=dm.Text(), nullable=True, immutable=False, auto_increment=False)
            },
            last_updated_time=1,
            created_time=1,
            is_global=False,
            used_for="node",
            constraints={},
            description=None,
            name=None,
            indexes={},
        )

        container_b = dm.Container(
            space="my_space",
            external_id="ContainerB",
            properties={
                "parent": dm.ContainerProperty(
                    type=dm.DirectRelation(container=dm.ContainerId("my_space", "ContainerA")),
                    nullable=True,
                    immutable=False,
                    auto_increment=False,
                )
            },
            last_updated_time=1,
            created_time=1,
            is_global=False,
            used_for="node",
            constraints={},
            description=None,
            name=None,
            indexes={},
        )

        # Create views
        view_a = dm.View(
            space="my_space",
            external_id="ViewA",
            version="v1",
            properties={
                "name": dm.MappedProperty(
                    container=dm.ContainerId("my_space", "ContainerA"),
                    container_property_identifier="name",
                    type=dm.Text(),
                    nullable=True,
                    immutable=False,
                    auto_increment=False,
                )
            },
            last_updated_time=1,
            created_time=1,
            is_global=False,
            used_for="node",
            writable=True,
            description=None,
            name=None,
            filter=None,
            implements=None,
        )

        view_b = dm.View(
            space="my_space",
            external_id="ViewB",
            version="v1",
            properties={
                "parent": dm.MappedProperty(
                    container=dm.ContainerId("my_space", "ContainerB"),
                    container_property_identifier="parent",
                    type=dm.DirectRelation(),
                    nullable=True,
                    immutable=False,
                    auto_increment=False,
                )
            },
            last_updated_time=1,
            created_time=1,
            is_global=False,
            used_for="node",
            writable=True,
            description=None,
            name=None,
            filter=None,
            implements=None,
        )

        toolkit_client_approval.append(dm.View, [view_a, view_b])
        toolkit_client_approval.append(dm.Container, [container_a, container_b])

        # Create selectors in reverse dependency order
        selector_b = InstanceSpaceSelector(
            instance_space="my_space",
            view=SelectedView(space="my_space", external_id="ViewB", version="v1"),
            type="instanceSpace",
        )
        selector_a = InstanceSpaceSelector(
            instance_space="my_space",
            view=SelectedView(space="my_space", external_id="ViewA", version="v1"),
            type="instanceSpace",
        )

        data_files_by_selector: dict[Selector, list[Path]] = {
            selector_b: [Path("dummy_b.json")],  # type: ignore[reportUnhashable]
            selector_a: [Path("dummy_a.json")],  # type: ignore[reportUnhashable]
        }

        result = cmd._prepare(data_files_by_selector, toolkit_client_approval.mock_client)

        # Should be reordered with ViewA before ViewB
        result_keys = list(result.keys())
        assert result_keys.index(selector_a) < result_keys.index(selector_b)

    def test_prepare_preserves_non_instance_selectors(self, toolkit_client_approval: ApprovalToolkitClient) -> None:
        """Test that _prepare preserves selectors that are not InstanceSpaceSelectors."""
        cmd = UploadCommand(silent=True, skip_tracking=True)

        # Create a mix of instance and raw selectors
        raw_selector = RawTableSelector(
            table=SelectedTable(db_name="test_db", table_name="test_table"), type="rawTable"
        )
        instance_selector = InstanceSpaceSelector(
            instance_space="my_space",
            view=None,
            type="instanceSpace",
        )

        data_files_by_selector: dict[Selector, list[Path]] = {
            raw_selector: [Path("raw_data.json")],  # type: ignore[reportUnhashable]
            instance_selector: [Path("instance_data.json")],  # type: ignore[reportUnhashable]
        }

        result = cmd._prepare(data_files_by_selector, toolkit_client_approval.mock_client)

        # Both selectors should be preserved
        assert raw_selector in result
        assert instance_selector in result
        assert len(result) == 2
