import sqlite3
import time
from collections.abc import Callable
from contextlib import closing
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.asset import AssetRequest
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    BooleanProperty,
    ContainerPropertyDefinition,
    ContainerRequest,
    InstanceSource,
    Int64Property,
    NodeId,
    NodeRequest,
    SpaceResponse,
    TextProperty,
    ViewCorePropertyRequest,
    ViewId,
    ViewRequest,
)
from cognite_toolkit._cdf_tk.client.resource_classes.dataset import DataSetRequest, DataSetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.raw import RAWDatabaseRequest, RAWRowRequest
from cognite_toolkit._cdf_tk.commands import DownloadCommand, UploadCommand
from cognite_toolkit._cdf_tk.constants import DATA_RESOURCE_DIR
from cognite_toolkit._cdf_tk.dataio import AssetDataIO, InstanceIO, RawIO, TableDataIO
from cognite_toolkit._cdf_tk.dataio.selectors import (
    DataSetSelector,
    InstanceViewSelector,
    RawTableSelector,
    SelectedTable,
    SelectedView,
    Selector,
)
from cognite_toolkit._cdf_tk.feature_flags import FeatureFlag, Flags
from cognite_toolkit._cdf_tk.utils.fileio import SQLiteReader

SQLITE_RAW_DB = "toolkit_smoke_sqlite_db"
SQLITE_RAW_TABLE = "sqlite_round_trip"
SQLITE_PREFIX = "toolkit_test_sqlite_round_trip"


@pytest.fixture
def enable_extend_download(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(FeatureFlag, "is_enabled", lambda flag: flag is Flags.EXTEND_DOWNLOAD)


@pytest.fixture
def sqlite_raw_rows(toolkit_client: ToolkitClient) -> list[RAWRowRequest]:
    existing = {db.name for batch in toolkit_client.raw.databases.iterate(limit=None) for db in batch}
    if SQLITE_RAW_DB not in existing:
        toolkit_client.raw.databases.create([RAWDatabaseRequest(name=SQLITE_RAW_DB)])
    rows = [
        RAWRowRequest(
            db_name=SQLITE_RAW_DB,
            table_name=SQLITE_RAW_TABLE,
            key=f"row_{no}",
            columns={
                "name": f"Pump {no}",
                "count": no,
                "ratio": no + 0.5,
                "active": no % 2 == 0,
                "tags": ["pump", f"tag_{no}"],
                "meta": {"unit": "bar", "level": no},
            },
        )
        for no in range(3)
    ]
    toolkit_client.raw.tables.rows.create(rows, ensure_parent=True)
    return rows


def get_or_create_data_set(client: ToolkitClient, external_id: str) -> DataSetResponse:
    dataset = DataSetRequest(external_id=external_id, name=external_id)
    retrieved = client.datasets.retrieve([dataset.as_id()], ignore_unknown_ids=True)
    return retrieved[0] if retrieved else client.datasets.create([dataset])[0]


@pytest.fixture
def sqlite_assets(toolkit_client: ToolkitClient) -> list[AssetRequest]:
    """Source assets that the tests never modify, as recreating them makes the aggregated count lag."""
    data_set = get_or_create_data_set(toolkit_client, SQLITE_PREFIX)
    root_external_id = f"{SQLITE_PREFIX}_root"
    assets = [
        AssetRequest(external_id=root_external_id, name="Root", data_set_id=data_set.id, metadata={"area": "north"}),
        AssetRequest(
            external_id=f"{SQLITE_PREFIX}_child",
            name="Child",
            parent_external_id=root_external_id,
            data_set_id=data_set.id,
            metadata={"area": "north", "type": "pump"},
        ),
    ]
    existing = toolkit_client.assets.retrieve([asset.as_id() for asset in assets], ignore_unknown_ids=True)
    existing_ids = {asset.external_id for asset in existing}
    if missing := [asset for asset in assets if asset.external_id not in existing_ids]:
        toolkit_client.assets.create(missing)
    return assets


@pytest.fixture
def sqlite_view(toolkit_client: ToolkitClient, smoke_space: SpaceResponse) -> ViewId:
    container = ContainerRequest(
        space=smoke_space.space,
        external_id=f"{SQLITE_PREFIX}_container",
        used_for="node",
        properties={
            "name": ContainerPropertyDefinition(type=TextProperty()),
            "count": ContainerPropertyDefinition(type=Int64Property()),
            "active": ContainerPropertyDefinition(type=BooleanProperty()),
        },
    )
    if not toolkit_client.containers.retrieve([container.as_id()]):
        toolkit_client.containers.create([container])
    view = ViewRequest(
        space=smoke_space.space,
        external_id=f"{SQLITE_PREFIX}_view",
        version="v1",
        properties={
            prop_id: ViewCorePropertyRequest(container=container.as_id(), container_property_identifier=prop_id)
            for prop_id in container.properties
        },
    )
    if not toolkit_client.views.retrieve([view.as_id()]):
        toolkit_client.views.create([view])
    return view.as_id()


class TestSQLiteDownload:
    def test_download_raw_table_as_sqlite(
        self, toolkit_client: ToolkitClient, sqlite_raw_rows: list[RAWRowRequest], tmp_path: Path
    ) -> None:
        cmd = DownloadCommand(silent=True, skip_tracking=True)
        selector = RawTableSelector(
            table=SelectedTable(db_name=SQLITE_RAW_DB, table_name=SQLITE_RAW_TABLE),
            download_dir_name=SQLITE_RAW_DB,
        )
        cmd.download(
            [selector],
            RawIO(toolkit_client),
            output_dir=tmp_path,
            verbose=False,
            file_format=".sqlite",
            compression="none",
            limit=None,
        )

        database = tmp_path / f"{selector.kind}.sqlite"
        table_name = f"{SQLITE_RAW_DB}.{SQLITE_RAW_TABLE}"
        assert {
            "tables": [(table.name, table.metadata) for table in SQLiteReader.list_tables(database)],
            "count": SQLiteReader(database, table_name).count(),
            "config_files": sorted(file.name for file in (tmp_path / DATA_RESOURCE_DIR).rglob("*.yaml")),
        } == {
            "tables": [(table_name, selector.model_dump(mode="json", by_alias=True))],
            "count": len(sqlite_raw_rows),
            "config_files": [f"{SQLITE_RAW_DB}.Database.yaml", f"{table_name}.Table.yaml"],
        }


class TestSQLiteRoundTrip:
    @staticmethod
    def download_and_upload(
        client: ToolkitClient,
        selector: Selector,
        io: TableDataIO,
        tmp_path: Path,
        before_upload: Callable[[], object],
        expected_count: int,
    ) -> None:
        # Counts come from eventually consistent aggregates, and the download is skipped when the count is zero.
        for _ in range(120):
            if io.count(selector) == expected_count:
                break
            time.sleep(1)
        DownloadCommand(silent=True, skip_tracking=True).download(
            selectors=[selector],
            io=io,
            output_dir=tmp_path,
            verbose=False,
            file_format=".sqlite",
            compression="none",
            limit=None,
        )
        assert list(tmp_path.glob("*.sqlite")), "Expected a SQLite database to be downloaded"
        before_upload()
        UploadCommand(silent=True, skip_tracking=True).upload(
            tmp_path, client, deploy_resources=False, dry_run=False, verbose=False
        )

    @pytest.mark.usefixtures("enable_extend_download")
    def test_raw_round_trip(
        self, toolkit_client: ToolkitClient, sqlite_raw_rows: list[RAWRowRequest], tmp_path: Path
    ) -> None:
        selector = RawTableSelector(
            table=SelectedTable(db_name=SQLITE_RAW_DB, table_name=SQLITE_RAW_TABLE),
            key="key",
            download_dir_name=SQLITE_RAW_DB,
        )

        self.download_and_upload(
            toolkit_client,
            selector,
            RawIO(toolkit_client),
            tmp_path,
            lambda: toolkit_client.raw.tables.rows.delete([row.as_id() for row in sqlite_raw_rows]),
            expected_count=len(sqlite_raw_rows),
        )

        uploaded = toolkit_client.raw.tables.rows.list(SQLITE_RAW_DB, SQLITE_RAW_TABLE)
        assert {row.key: row.columns for row in uploaded} == {row.key: row.columns for row in sqlite_raw_rows}

    @pytest.mark.usefixtures("enable_extend_download")
    def test_assets_round_trip(
        self, toolkit_client: ToolkitClient, sqlite_assets: list[AssetRequest], tmp_path: Path
    ) -> None:
        target_data_set = get_or_create_data_set(toolkit_client, f"{SQLITE_PREFIX}_copy")
        copy_ids = [ExternalId(external_id=f"{asset.external_id}_copy") for asset in sqlite_assets]
        toolkit_client.assets.delete(copy_ids, recursive=True, ignore_unknown_ids=True)
        selector = DataSetSelector(kind="Assets", data_set_external_id=SQLITE_PREFIX, download_dir_name="assets")

        def copy_to_target_data_set() -> None:
            database = tmp_path / f"{selector.kind}.sqlite"
            table = SQLiteReader.list_tables(database)[0].name
            with closing(sqlite3.connect(database)) as connection, connection:
                connection.execute(
                    f'UPDATE "{table}" SET "externalId" = "externalId" || ?, '
                    '"parentExternalId" = "parentExternalId" || ?, "dataSetExternalId" = ?',
                    ("_copy", "_copy", target_data_set.external_id),
                )

        try:
            self.download_and_upload(
                toolkit_client,
                selector,
                AssetDataIO(toolkit_client),
                tmp_path,
                copy_to_target_data_set,
                expected_count=len(sqlite_assets),
            )
            uploaded = toolkit_client.assets.retrieve(copy_ids, ignore_unknown_ids=True)
        finally:
            toolkit_client.assets.delete(copy_ids, recursive=True, ignore_unknown_ids=True)

        id_by_external_id = {asset.external_id: asset.id for asset in uploaded}
        assert [
            (asset.external_id, asset.name, asset.metadata, asset.data_set_id, asset.parent_id) for asset in uploaded
        ] == [
            (
                f"{asset.external_id}_copy",
                asset.name,
                asset.metadata,
                target_data_set.id,
                id_by_external_id.get(f"{asset.parent_external_id}_copy"),
            )
            for asset in sqlite_assets
        ]

    @pytest.mark.usefixtures("enable_extend_download")
    def test_instances_round_trip(
        self, toolkit_client: ToolkitClient, smoke_space: SpaceResponse, sqlite_view: ViewId, tmp_path: Path
    ) -> None:
        properties = {"name": "SQLite node", "count": 42, "active": True}
        node_id = NodeId(space=smoke_space.space, external_id=f"{SQLITE_PREFIX}_node")
        toolkit_client.instances.create(
            [
                NodeRequest(
                    space=node_id.space,
                    external_id=node_id.external_id,
                    sources=[InstanceSource(source=sqlite_view, properties=properties)],
                )
            ]
        )
        selector = InstanceViewSelector(
            view=SelectedView(
                space=sqlite_view.space, external_id=sqlite_view.external_id, version=sqlite_view.version
            ),
            instance_type="node",
            instance_spaces=(smoke_space.space,),
            download_dir_name="instances",
        )

        self.download_and_upload(
            toolkit_client,
            selector,
            InstanceIO(toolkit_client),
            tmp_path,
            lambda: toolkit_client.instances.delete([node_id]),
            expected_count=1,
        )

        uploaded = toolkit_client.instances.retrieve([node_id], source=sqlite_view)
        assert [(node.as_id(), (node.properties or {}).get(sqlite_view)) for node in uploaded] == [
            (node_id, properties)
        ]
