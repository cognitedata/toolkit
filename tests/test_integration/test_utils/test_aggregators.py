import pytest
from cognite.client.data_classes import (
    Asset,
    AssetWrite,
    DataSetList,
    DataSetWrite,
    DataSetWriteList,
    RowWrite,
    Transformation,
    TransformationDestination,
    TransformationJob,
    TransformationWrite,
)

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.raw import RawDatabase, RawDatabaseList, RawTable, RawTableList
from cognite_toolkit._cdf_tk.loaders import RawDatabaseLoader, RawTableLoader
from cognite_toolkit._cdf_tk.utils.aggregators import AssetAggregator


@pytest.fixture(scope="session")
def raw_db(toolkit_client: ToolkitClient) -> str:
    loader = RawDatabaseLoader.create_loader(toolkit_client)
    db_name = "toolkit_aggregators_test_db"
    if not loader.retrieve([RawDatabase(db_name=db_name)]):
        loader.create(RawDatabaseList([RawDatabase(db_name=db_name)]))
    return db_name


@pytest.fixture(scope="session")
def two_datasets(toolkit_client: ToolkitClient) -> DataSetList:
    datasets = DataSetWriteList(
        [
            DataSetWrite(external_id="toolkit_aggregators_test_dataset_1", name="Toolkit Aggregators Test Dataset 1"),
            DataSetWrite(external_id="toolkit_aggregators_test_dataset_2", name="Toolkit Aggregators Test Dataset 2"),
        ]
    )
    retrieved = toolkit_client.data_sets.retrieve_multiple(
        external_ids=datasets.as_external_ids(), ignore_unknown_ids=True
    )
    if not retrieved:
        return toolkit_client.data_sets.create(datasets)

    return retrieved


@pytest.fixture(scope="session")
def root_asset(toolkit_client: ToolkitClient, two_datasets: DataSetList) -> Asset:
    root_asset = AssetWrite(
        name="Toolkit Aggregators Test Root Asset",
        external_id="toolkit_aggregators_test_root_asset",
        data_set_id=two_datasets[0].id,
    )
    retrieved = toolkit_client.assets.retrieve(external_id=root_asset.external_id)
    if retrieved is None:
        return toolkit_client.assets.create(root_asset)
    return retrieved


def create_raw_table_with_data(client: ToolkitClient, table: RawTable, rows: list[RowWrite]) -> None:
    loader = RawTableLoader.create_loader(client)
    existing_tables = loader.retrieve([table])
    if not existing_tables:
        loader.create(RawTableList([table]))
    data = client.raw.rows.list(table.db_name, table.table_name, limit=len(rows))
    if not data:
        client.raw.rows.insert(table.db_name, table.table_name, rows)


@pytest.fixture(scope="session")
def assets(
    toolkit_client: ToolkitClient, raw_db: str, root_asset: Asset, two_datasets: DataSetList
) -> tuple[Transformation, int]:
    table_name = "toolkit_aggregators_test_table_assets"
    rows = [
        RowWrite(
            key=f"asset_00{i}",
            columns={
                "name": f"Asset 00{i}",
                "externalId": f"asset_00{i}",
                "dataSetId": two_datasets[0].id,
                "parentExternalId": root_asset.external_id,
            },
        )
        for i in range(1, 6)
    ]
    create_raw_table_with_data(
        toolkit_client,
        RawTable(db_name=raw_db, table_name=table_name),
        rows,
    )

    transformation = TransformationWrite(
        external_id="toolkit_aggregators_test_asset_transformation",
        name="Toolkit Aggregators Test Asset Transformation",
        destination=TransformationDestination("assets"),
        query=f"""SELECT name as name, externalId as externalId, dataset_external_id('{two_datasets[0].external_id}') as dataSetId, parentExternalId as parentExternalId
FROM `{raw_db}`.`{table_name}`""",
        ignore_null_fields=True,
    )
    retrieved = toolkit_client.transformations.retrieve(external_id=transformation.external_id)
    if retrieved is None:
        created = toolkit_client.transformations.create(transformation)
    else:
        created = retrieved
    if created.last_finished_job is None:
        nonce = toolkit_client.iam.sessions.create(session_type="ONESHOT_TOKEN_EXCHANGE")
        response = toolkit_client.post(
            url=f"/api/v1/projects/{toolkit_client.config.project}/transformations/run",
            json={
                "id": created.id,
                "nonce": {
                    "sessionId": nonce.id,
                    "nonce": nonce.nonce,
                    "cdfProjectName": toolkit_client.config.project,
                },
            },
        )
        job = TransformationJob._load(response.json(), cognite_client=toolkit_client)
        job.wait()

    return created, len(rows) + 1  # Asset count + root asset.


class TestAggregators:
    def test_asset_aggregations(
        self,
        toolkit_client: ToolkitClient,
        assets: tuple[Transformation, int],
        root_asset: Asset,
        two_datasets: DataSetList,
    ) -> None:
        transformation, expected_asset_count = assets
        data_set = two_datasets[0].external_id
        root = root_asset.external_id
        aggregator = AssetAggregator(toolkit_client)

        actual_count = aggregator.count(root)
        assert actual_count == expected_asset_count

        used_data_sets = aggregator.used_data_sets(root)
        assert used_data_sets == [data_set]

        used_transformations = aggregator.used_transformations(used_data_sets)

        assert len(used_transformations) == 1
        assert used_transformations[0].external_id == transformation.external_id
