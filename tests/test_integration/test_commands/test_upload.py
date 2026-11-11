import sqlite3
import time
from collections.abc import Callable
from contextlib import closing
from datetime import datetime
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
    Space,
    TextProperty,
    ViewCorePropertyRequest,
    ViewId,
    ViewRequest,
)
from cognite_toolkit._cdf_tk.client.resource_classes.datapoints import (
    AggregateDatapointsResponse,
    DatapointsQueryRequest,
)
from cognite_toolkit._cdf_tk.client.resource_classes.dataset import DataSetRequest, DataSetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.raw import RAWDatabaseResponse, RAWRowRequest
from cognite_toolkit._cdf_tk.client.resource_classes.timeseries import TimeSeriesRequest, TimeSeriesResponse
from cognite_toolkit._cdf_tk.commands import DownloadCommand, UploadCommand
from cognite_toolkit._cdf_tk.dataio import AssetDataIO, DatapointsIO, InstanceIO, RawIO, TableDataIO
from cognite_toolkit._cdf_tk.dataio.selectors import (
    DataPointsDataSetSelector,
    DataPointsFileSelector,
    DataSetSelector,
    InstanceViewSelector,
    RawTableSelector,
    SelectedTable,
    SelectedView,
    Selector,
)
from cognite_toolkit._cdf_tk.dataio.selectors._datapoints import ExternalIdColumn, InternalIdColumn
from cognite_toolkit._cdf_tk.feature_flags import FeatureFlag, Flags
from cognite_toolkit._cdf_tk.utils.fileio import SQLiteReader


@pytest.fixture(scope="session")
def upload_dataset(toolkit_client: ToolkitClient) -> DataSetResponse:
    dataset = DataSetRequest(
        external_id="toolkit_tests_dataset", name="Toolkit Test DataSet", description="Toolkit DataSet used in tests"
    )
    retrieved = toolkit_client.datasets.retrieve([dataset.as_id()], ignore_unknown_ids=True)
    if retrieved:
        return retrieved[0]
    return toolkit_client.datasets.create([dataset])[0]


@pytest.fixture(scope="session")
def two_timeseries(
    toolkit_client: ToolkitClient, upload_dataset: DataSetResponse
) -> tuple[TimeSeriesResponse, TimeSeriesResponse]:
    requests = [
        TimeSeriesRequest(
            name="Test TimeSeries for Datapoints IO",
            external_id="test_timeseries_datapoints_io",
            is_step=False,
            is_string=False,
            data_set_id=upload_dataset.id,
        ),
        TimeSeriesRequest(
            name="Test TimeSeries 2 for Datapoints IO",
            external_id="test_timeseries_2_datapoints_io",
            is_step=False,
            is_string=True,
            data_set_id=upload_dataset.id,
        ),
    ]
    result: list[TimeSeriesResponse] = []
    for request in requests:
        assert request.external_id is not None
        retrieved = toolkit_client.timeseries.retrieve(
            [ExternalId(external_id=request.external_id)], ignore_unknown_ids=True
        )
        result.append(retrieved[0] if retrieved else toolkit_client.timeseries.create([request])[0])
    return result[0], result[1]


class TestUploadCommand:
    def test_upload_datapoints(
        self,
        toolkit_client: ToolkitClient,
        two_timeseries: tuple[TimeSeriesResponse, TimeSeriesResponse],
        tmp_path: Path,
    ) -> None:
        ts1, ts2 = two_timeseries
        ts1_external_id = ts1.external_id
        ts2_external_id = ts2.external_id
        assert ts1_external_id is not None
        assert ts2_external_id is not None
        assert ts1.is_string is False
        assert ts2.is_string is True
        selector = DataPointsFileSelector(
            timestamp_column="timestamp",
            columns=(
                ExternalIdColumn(
                    column="value",
                    external_id=ts1_external_id,
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

        results = toolkit_client.timeseries.datapoints.retrieve(
            [
                DatapointsQueryRequest(external_id=ts1_external_id),
                DatapointsQueryRequest(external_id=ts2_external_id),
            ],
            start=datetime.fromisoformat("2024-01-01T00:00:00Z"),
            end=datetime.fromisoformat("2024-01-01T00:00:10Z"),
        )
        assert len(results) == 2
        for result in results:
            assert len(result.datapoints) == 10, f"Expected 10 datapoints, got {len(result.datapoints)}"

    def test_upload_download_datapoints(
        self,
        toolkit_client: ToolkitClient,
        two_timeseries: tuple[TimeSeriesResponse, TimeSeriesResponse],
        tmp_path: Path,
    ) -> None:
        ts1, _ = two_timeseries
        client = toolkit_client
        ts1_external_id = ts1.external_id
        data_set_id = ts1.data_set_id
        assert ts1_external_id is not None
        assert data_set_id is not None
        assert ts1.is_string is False
        data_set_external_id = client.lookup.data_sets.external_id(data_set_id)
        assert data_set_external_id is not None

        upload_dir = tmp_path / "upload"
        upload_dir.mkdir(parents=True, exist_ok=True)
        selector = DataPointsDataSetSelector(
            data_set_external_id=data_set_external_id,
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
            rows.append(f"{ts1_external_id},{timestamp_ms},{i * 5}.0")
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

        aggregate_results = toolkit_client.timeseries.datapoints.retrieve(
            [DatapointsQueryRequest(external_id=ts1_external_id)],
            start=datetime.fromisoformat("1989-01-01T00:00:00Z"),
            end=datetime.fromisoformat("1989-01-02T00:00:00Z"),
            aggregates=["count"],
            granularity="1d",
        )
        assert len(aggregate_results) == 1
        aggregate_result = aggregate_results[0]
        assert isinstance(aggregate_result, AggregateDatapointsResponse)
        assert aggregate_result.datapoints, "Expected aggregate datapoints"
        count = aggregate_result.datapoints[0].count
        assert count == 10, f"Expected 10 datapoints, got {count}"

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


SQLITE_RAW_TABLE = "sqlite_round_trip"
SQLITE_PREFIX = "toolkit_test_sqlite_round_trip"


@pytest.fixture
def enable_extend_download(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(FeatureFlag, "is_enabled", lambda flag: flag is Flags.EXTEND_DOWNLOAD)


@pytest.fixture
def sqlite_raw_rows(toolkit_client: ToolkitClient, toolkit_raw_database: RAWDatabaseResponse) -> list[RAWRowRequest]:
    rows = [
        RAWRowRequest(
            db_name=toolkit_raw_database.name,
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
def sqlite_view(toolkit_client: ToolkitClient, toolkit_space: Space) -> ViewId:
    container = ContainerRequest(
        space=toolkit_space.space,
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
        space=toolkit_space.space,
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
        db_name = sqlite_raw_rows[0].db_name
        selector = RawTableSelector(
            table=SelectedTable(db_name=db_name, table_name=SQLITE_RAW_TABLE), key="key", download_dir_name=db_name
        )

        self.download_and_upload(
            toolkit_client,
            selector,
            RawIO(toolkit_client),
            tmp_path,
            lambda: toolkit_client.raw.tables.rows.delete([row.as_id() for row in sqlite_raw_rows]),
            expected_count=len(sqlite_raw_rows),
        )

        uploaded = toolkit_client.raw.tables.rows.list(db_name, SQLITE_RAW_TABLE)
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
        self, toolkit_client: ToolkitClient, toolkit_space: Space, sqlite_view: ViewId, tmp_path: Path
    ) -> None:
        properties = {"name": "SQLite node", "count": 42, "active": True}
        node_id = NodeId(space=toolkit_space.space, external_id=f"{SQLITE_PREFIX}_node")
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
            instance_spaces=(toolkit_space.space,),
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
