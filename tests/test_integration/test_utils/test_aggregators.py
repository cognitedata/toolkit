import pytest
from cognite.client.data_classes import (
    Asset,
    AssetList,
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
from cognite_toolkit._cdf_tk.utils.aggregators import (
    AssetAggregator,
    AssetCentricAggregator,
    EventAggregator,
    FileAggregator,
    SequenceFilter,
    TimeSeriesAggregator,
)

ASSET_TRANSFORMATION = "toolkit_aggregators_test_asset_transformation"
EVENT_TRANSFORMATION = "toolkit_aggregators_test_event_transformation"
FILE_TRANSFORMATION = "toolkit_aggregators_test_file_transformation"
TIMESERIES_TRANSFORMATION = "toolkit_aggregators_test_timeseries_transformation"
SEQUENCE_TRANSFORMATION = "toolkit_aggregators_test_sequence_transformation"

ASSET_DATASET = "toolkit_aggregators_test_dataset_1"
EVENT_DATASET = "toolkit_aggregators_test_dataset_1"
FILE_DATASET = "toolkit_aggregators_test_dataset_2"
TIMESERIES_DATASET = "toolkit_aggregators_test_dataset_2"
SEQUENCE_DATASET = "toolkit_aggregators_test_dataset_2"

ASSET_COUNT = 6
EVENT_COUNT = 10
FILE_COUNT = 3
TIMESERIES_COUNT = 20
SEQUENCE_COUNT = 2


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


def upsert_transformation_with_run(
    toolkit_client: ToolkitClient, transformation: TransformationWrite
) -> Transformation:
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
        assert job.error is None
    return created


@pytest.mark.usefixtures("two_datasets")
@pytest.fixture(scope="session")
def assets(toolkit_client: ToolkitClient, raw_db: str, root_asset: Asset) -> Transformation:
    table_name = "toolkit_aggregators_test_table_assets"
    rows = [
        RowWrite(
            key=f"asset_00{i}",
            columns={
                "name": f"Asset 00{i}",
                "externalId": f"asset_00{i}",
                "parentExternalId": root_asset.external_id,
            },
        )
        for i in range(1, ASSET_COUNT - 1 + 1)  # -1 for root asset, +1 for inclusive range
    ]
    create_raw_table_with_data(
        toolkit_client,
        RawTable(db_name=raw_db, table_name=table_name),
        rows,
    )

    transformation = TransformationWrite(
        external_id=ASSET_TRANSFORMATION,
        name="Toolkit Aggregators Test Asset Transformation",
        destination=TransformationDestination("assets"),
        query=f"""SELECT name as name, externalId as externalId, dataset_id('{ASSET_DATASET}') as dataSetId, parentExternalId as parentExternalId
FROM `{raw_db}`.`{table_name}`""",
        ignore_null_fields=True,
    )
    created = upsert_transformation_with_run(toolkit_client, transformation)
    return created


@pytest.mark.usefixtures("assets")
@pytest.fixture(scope="session")
def asset_list(toolkit_client: ToolkitClient, root_asset: Asset) -> AssetList:
    return toolkit_client.assets.list(
        asset_subtree_ids=[root_asset.id],
    )


@pytest.mark.usefixtures("two_datasets")
@pytest.fixture(scope="session")
def events(toolkit_client: ToolkitClient, raw_db: str, asset_list: AssetList) -> Transformation:
    table_name = "toolkit_aggregators_test_table_events"
    rows = [
        RowWrite(
            key=f"event_00{i}",
            columns={
                "externalId": f"event_00{i}",
                "name": f"Event 00{i}",
                "startTime": 1000000000 + i * 1000,  # Staggered start times
                "endTime": 1000000000 + i * 1000 + 500,  # 500ms duration
                "assetIds": [asset_list[i % len(asset_list)].id],
            },
        )
        for i in range(1, EVENT_COUNT + 1)  # +1 for inclusive range
    ]
    create_raw_table_with_data(
        toolkit_client,
        RawTable(db_name=raw_db, table_name=table_name),
        rows,
    )
    transformation = TransformationWrite(
        external_id=EVENT_TRANSFORMATION,
        name="Toolkit Aggregators Test Event Transformation",
        destination=TransformationDestination("events"),
        query=f"""SELECT externalId as externalId, name as name, timestamp_millis(startTime) as startTime, timestamp_millis(endTime) as endTime,
assetIds as assetIds, dataset_id('{EVENT_DATASET}') as dataSetId
FROM `{raw_db}`.`{table_name}`""",
        ignore_null_fields=True,
    )
    created = upsert_transformation_with_run(toolkit_client, transformation)
    return created


@pytest.fixture(scope="session")
def files(
    toolkit_client: ToolkitClient, raw_db: str, two_datasets: DataSetList, asset_list: AssetList
) -> Transformation:
    table_name = "toolkit_aggregators_test_table_files"
    rows = [
        RowWrite(
            key=f"file_00{i}",
            columns={
                "externalId": f"file_00{i}",
                "name": f"File 00{i}",
                "assetIds": [asset_list[i % len(asset_list)].id],  # Assign to one of the assets
                "mimeType": "application/text",
            },
        )
        for i in range(1, FILE_COUNT + 1)  # +1 for inclusive range
    ]
    # all_files = toolkit_client.files.list(limit=-1)
    # to_delete = [file for file in all_files if file.external_id and file.external_id.startswith("file_00")]
    # if to_delete:
    #     toolkit_client.files.delete(id=[file.id for file in to_delete], ignore_unknown_ids=True)

    create_raw_table_with_data(
        toolkit_client,
        RawTable(db_name=raw_db, table_name=table_name),
        rows,
    )
    transformation = TransformationWrite(
        external_id=FILE_TRANSFORMATION,
        name="Toolkit Aggregators Test File Transformation",
        destination=TransformationDestination("files"),
        query=f"""SELECT externalId as externalId, name as name, assetIds as assetIds,
dataset_id('{FILE_DATASET}') as dataSetId, mimeType as mimeType
FROM `{raw_db}`.`{table_name}`""",
        ignore_null_fields=True,
    )
    created = upsert_transformation_with_run(toolkit_client, transformation)

    # Upload content for the files
    external_ids = [row.columns["externalId"] for row in rows]
    filemetadata = toolkit_client.files.retrieve_multiple(external_ids=external_ids)
    is_uploaded_by_external_id = {
        file.external_id: file.uploaded for file in filemetadata if file.external_id is not None
    }
    for row in rows:
        external_id = row.columns["externalId"]
        if is_uploaded_by_external_id.get(external_id):
            continue
        file_content = f"Content of {external_id}"
        toolkit_client.files.upload_content_bytes(file_content, external_id=external_id)

    return created


@pytest.fixture(scope="session")
def time_series(
    toolkit_client: ToolkitClient, raw_db: str, two_datasets: DataSetList, asset_list: AssetList
) -> Transformation:
    table_name = "toolkit_aggregators_test_table_time_series"
    rows = [
        RowWrite(
            key=f"timeseries_00{i}",
            columns={
                "externalId": f"timeseries_00{i}",
                "name": f"Time Series 00{i}",
                "assetId": asset_list[i % len(asset_list)].id,  # Assign to one of the assets
                "isString": False,
                "isStep": False,
            },
        )
        for i in range(1, TIMESERIES_COUNT + 1)  # +1 for inclusive range
    ]
    create_raw_table_with_data(
        toolkit_client,
        RawTable(db_name=raw_db, table_name=table_name),
        rows,
    )
    transformation = TransformationWrite(
        external_id=TIMESERIES_TRANSFORMATION,
        name="Toolkit Aggregators Test Time Series Transformation",
        destination=TransformationDestination("timeseries"),
        query=f"""SELECT externalId as externalId, name as name, assetId as assetId, isString as isString, isStep as isStep,
dataset_id('{TIMESERIES_DATASET}') as dataSetId
FROM `{raw_db}`.`{table_name}`""",
        ignore_null_fields=True,
    )
    created = upsert_transformation_with_run(toolkit_client, transformation)
    return created


@pytest.fixture(scope="session")
def sequences(
    toolkit_client: ToolkitClient, raw_db: str, two_datasets: DataSetList, asset_list: AssetList
) -> Transformation:
    table_name = "toolkit_aggregators_test_table_sequences"
    rows = [
        RowWrite(
            key=f"sequence_00{i}",
            columns={
                "externalId": f"sequence_00{i}",
                "name": f"Sequence 00{i}",
                "assetId": asset_list[i % len(asset_list)].id,  # Assign to one of the assets
                "columns": [
                    {"name": f"Column {j}", "valueType": "STRING", "externalId": f"column_{j}"} for j in range(1, 4)
                ],
            },
        )
        for i in range(1, SEQUENCE_COUNT + 1)  # +1 for inclusive range
    ]
    create_raw_table_with_data(
        toolkit_client,
        RawTable(db_name=raw_db, table_name=table_name),
        rows,
    )
    transformation = TransformationWrite(
        external_id=SEQUENCE_TRANSFORMATION,
        name="Toolkit Aggregators Test Sequence Transformation",
        destination=TransformationDestination("sequences"),
        query=f"""SELECT externalId as externalId, name as name, assetId as assetId, columns as columns, dataset_id('{SEQUENCE_DATASET}') as dataSetId
FROM `{raw_db}`.`{table_name}`""",
        ignore_null_fields=True,
    )
    created = upsert_transformation_with_run(toolkit_client, transformation)
    return created


class TestAggregators:
    aggregators: tuple[tuple[type[AssetCentricAggregator], str, str, int], ...] = (
        (AssetAggregator, ASSET_TRANSFORMATION, ASSET_DATASET, ASSET_COUNT),
        (TimeSeriesAggregator, TIMESERIES_TRANSFORMATION, TIMESERIES_DATASET, TIMESERIES_COUNT),
        (EventAggregator, EVENT_TRANSFORMATION, EVENT_DATASET, EVENT_COUNT),
        (FileAggregator, FILE_TRANSFORMATION, FILE_DATASET, FILE_COUNT),
        (SequenceFilter, SEQUENCE_TRANSFORMATION, SEQUENCE_DATASET, SEQUENCE_COUNT),
    )

    @pytest.mark.usefixtures("assets", "events", "files", "time_series", "sequences")
    @pytest.mark.parametrize(
        "aggregator_class, expected_transformation_external_id, expected_dataset_external_id, expected_count",
        aggregators,
    )
    def test_aggregations(
        self,
        toolkit_client: ToolkitClient,
        aggregator_class: type[AssetCentricAggregator],
        expected_transformation_external_id: str,
        expected_dataset_external_id: str,
        expected_count: int,
        root_asset: Asset,
    ) -> None:
        root = root_asset.external_id
        aggregator = aggregator_class(toolkit_client)

        actual_count = aggregator.count(root)
        assert actual_count == expected_count

        used_data_sets = aggregator.used_data_sets(root)
        assert used_data_sets == [expected_dataset_external_id]

        transformation_count = aggregator.transformation_count()
        assert transformation_count >= 1  # We know at least one transformation is writing to the resource type.
        used_transformations = aggregator.used_transformations(used_data_sets)

        assert len(used_transformations) == 1
        assert used_transformations[0].external_id == expected_transformation_external_id
