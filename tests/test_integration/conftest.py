import os
import shutil
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from cognite.client import CogniteClient, global_config
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import (
    Asset,
    AssetList,
    AssetWrite,
    DataSet,
    DataSetList,
    DataSetWrite,
    DataSetWriteList,
    Function,
    FunctionSchedule,
    RowWrite,
    RowWriteList,
    Transformation,
    TransformationDestination,
    TransformationJob,
    TransformationWrite,
)
from cognite.client.data_classes.data_modeling import Space, SpaceApply
from dotenv import load_dotenv

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.data_classes.raw import RawDatabase, RawDatabaseList, RawTable, RawTableList
from cognite_toolkit._cdf_tk.commands import CollectCommand
from cognite_toolkit._cdf_tk.cruds import RawDatabaseCRUD, RawTableCRUD
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from cognite_toolkit._cdf_tk.utils.cdf import ThrottlerState, raw_row_count
from tests.constants import REPO_ROOT
from tests.test_integration.constants import (
    ASSET_COUNT,
    ASSET_DATASET,
    ASSET_TABLE,
    ASSET_TRANSFORMATION,
    EVENT_COUNT,
    EVENT_DATASET,
    EVENT_TABLE,
    EVENT_TRANSFORMATION,
    FILE_COUNT,
    FILE_DATASET,
    FILE_TABLE,
    FILE_TRANSFORMATION,
    SEQUENCE_COUNT,
    SEQUENCE_DATASET,
    SEQUENCE_TABLE,
    SEQUENCE_TRANSFORMATION,
    TIMESERIES_COUNT,
    TIMESERIES_DATASET,
    TIMESERIES_TABLE,
    TIMESERIES_TRANSFORMATION,
)

THIS_FOLDER = Path(__file__).resolve().parent
TMP_FOLDER = THIS_FOLDER / "tmp"


@pytest.fixture(scope="session")
def toolkit_client_config() -> ToolkitClientConfig:
    load_dotenv(REPO_ROOT / ".env", override=True)
    # Ensure that we do not collect data during tests
    cmd = CollectCommand()
    cmd.execute(action="opt-out")

    cdf_cluster = os.environ["CDF_CLUSTER"]
    credentials = OAuthClientCredentials(
        token_url=os.environ["IDP_TOKEN_URL"],
        client_id=os.environ["IDP_CLIENT_ID"],
        client_secret=os.environ["IDP_CLIENT_SECRET"],
        scopes=[f"https://{cdf_cluster}.cognitedata.com/.default"],
        audience=f"https://{cdf_cluster}.cognitedata.com",
    )
    global_config.disable_pypi_version_check = True
    return ToolkitClientConfig(
        client_name="cdf-toolkit-integration-tests",
        base_url=f"https://{cdf_cluster}.cognitedata.com",
        project=os.environ["CDF_PROJECT"],
        credentials=credentials,
        # We cannot commit auth to WorkflowTrigger and FunctionSchedules.
        is_strict_validation=False,
    )


@pytest.fixture(scope="session")
def cognite_client(toolkit_client_config: ToolkitClientConfig) -> CogniteClient:
    return CogniteClient(toolkit_client_config)


@pytest.fixture(scope="session")
def toolkit_client(toolkit_client_config: ToolkitClientConfig) -> ToolkitClient:
    return ToolkitClient(toolkit_client_config)


@pytest.fixture()
def max_two_workers():
    old = global_config.max_workers
    global_config.max_workers = 2
    yield
    global_config.max_workers = old


@pytest.fixture(scope="session")
def toolkit_client_with_pending_ids(toolkit_client_config: ToolkitClientConfig) -> ToolkitClient:
    """Returns a ToolkitClient configured to enable pending IDs."""
    return ToolkitClient(toolkit_client_config, enable_set_pending_ids=True)


@pytest.fixture(scope="session")
def env_vars(toolkit_client: ToolkitClient) -> EnvironmentVariables:
    env_vars = EnvironmentVariables.create_from_environment()
    # Ensure we use the client above that has CLIENT NAME set to the test name
    env_vars._client = toolkit_client
    return env_vars


@pytest.fixture(scope="session")
def toolkit_space(cognite_client: CogniteClient) -> Space:
    return cognite_client.data_modeling.spaces.apply(SpaceApply(space="toolkit_test_space"))


@pytest.fixture(scope="session")
def toolkit_dataset(cognite_client: CogniteClient) -> DataSet:
    """Returns the dataset name used for toolkit tests."""
    dataset = DataSetWrite(
        external_id="toolkit_tests_dataset", name="Toolkit Test DataSet", description="Toolkit DataSet used in tests"
    )
    retrieved = cognite_client.data_sets.retrieve(external_id=dataset.external_id)
    if retrieved is None:
        return cognite_client.data_sets.create(dataset)
    return retrieved


@pytest.fixture
def build_dir() -> Path:
    pidid = os.getpid()
    build_path = TMP_FOLDER / f"build-{pidid}"
    build_path.mkdir(exist_ok=True, parents=True)
    yield build_path
    shutil.rmtree(build_path, ignore_errors=True)


@pytest.fixture(scope="session")
def dev_cluster_client() -> ToolkitClient | None:
    """Returns a ToolkitClient configured for the development cluster."""
    dev_cluster_env = REPO_ROOT / "dev-cluster.env"
    if not dev_cluster_env.exists():
        pytest.skip("dev-cluster.env file not found, skipping tests that require dev cluster client.")
        return None
    env_content = dev_cluster_env.read_text(encoding="utf-8")
    env_vars = dict(
        line.strip().split("=")
        for line in env_content.splitlines()
        if line.strip() and not line.startswith("#") and "=" in line
    )
    cdf_cluster = env_vars["CDF_CLUSTER"]
    credentials = OAuthClientCredentials(
        token_url=env_vars["IDP_TOKEN_URL"],
        client_id=env_vars["IDP_CLIENT_ID"],
        client_secret=env_vars["IDP_CLIENT_SECRET"],
        scopes=[f"https://{cdf_cluster}.cognitedata.com/.default"],
        audience=f"https://{cdf_cluster}.cognitedata.com",
    )
    config = ToolkitClientConfig(
        client_name="cdf-toolkit-integration-tests",
        base_url=f"https://{cdf_cluster}.cognitedata.com",
        project=env_vars["CDF_PROJECT"],
        credentials=credentials,
        is_strict_validation=False,
    )
    return ToolkitClient(config, enable_set_pending_ids=True)


@pytest.fixture(scope="session")
def dummy_function(cognite_client: CogniteClient) -> Function:
    external_id = "integration_test_function_dummy"

    if existing := cognite_client.functions.retrieve(external_id=external_id):
        return existing

    def handle(client: CogniteClient, data: dict, function_call_info: dict) -> str:
        """
        [requirements]
        cognite-sdk>=7.37.0
        [/requirements]
        """
        print("Print statements will be shown in the logs.")
        print("Running with the following configuration:\n")
        return {
            "data": data,
            "functionInfo": function_call_info,
        }

    return cognite_client.functions.create(
        name="integration_test_function_dummy",
        function_handle=handle,
        external_id="integration_test_function_dummy",
    )


@pytest.fixture
def dummy_schedule(cognite_client: CogniteClient, dummy_function: Function) -> FunctionSchedule:
    name = "integration_test_schedule_dummy"
    if existing_list := cognite_client.functions.schedules.list(name=name):
        if len(existing_list) > 1:
            for existing in existing_list[1:]:
                cognite_client.functions.schedules.delete(existing.id)
        schedule = existing_list[0]
    else:
        schedule = cognite_client.functions.schedules.create(
            name=name,
            cron_expression="0 7 * * MON",
            description="Original description.",
            function_external_id=dummy_function.external_id,
        )
    if schedule.function_external_id is None:
        schedule.function_external_id = dummy_function.external_id
    if schedule.function_id is not None:
        schedule.function_id = None
    return schedule


@pytest.fixture()
def raw_data() -> RowWriteList:
    return RowWriteList(
        [
            RowWrite(
                key=f"row{i}",
                columns={
                    "StringCol": f"value{i % 3}",
                    "IntegerCol": i % 5,
                    "BooleanCol": [True, False][i % 2],
                    "FloatCol": i * 0.1,
                    "EmptyCol": None,
                    "ArrayCol": [i, i + 1, i + 2] if i % 2 == 0 else None,
                    "ObjectCol": {"nested_key": f"nested_value_{i}" if i % 2 == 0 else None},
                },
            )
            for i in range(10)
        ]
    )


@pytest.fixture()
def populated_raw_table(toolkit_client: ToolkitClient, raw_data: RowWriteList) -> RawTable:
    db_name = "toolkit_test_db"
    table_name = "toolkit_test_profiling_table"
    existing_dbs = toolkit_client.raw.databases.list(limit=-1)
    existing_table_names: set[str] = set()
    if db_name in {db.name for db in existing_dbs}:
        existing_table_names = {table.name for table in toolkit_client.raw.tables.list(db_name=db_name, limit=-1)}
    if table_name not in existing_table_names:
        toolkit_client.raw.rows.insert(db_name, table_name, raw_data, ensure_parent=True)
    return RawTable(db_name, table_name)


@pytest.fixture(scope="session")
def aggregator_raw_db(toolkit_client: ToolkitClient) -> str:
    loader = RawDatabaseCRUD.create_loader(toolkit_client)
    db_name = "toolkit_aggregators_test_db"
    if not loader.retrieve([RawDatabase(db_name=db_name)]):
        loader.create(RawDatabaseList([RawDatabase(db_name=db_name)]))
    return db_name


@pytest.fixture(scope="session")
def aggregator_two_datasets(toolkit_client: ToolkitClient) -> DataSetList:
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
def aggregator_root_asset(toolkit_client: ToolkitClient, aggregator_two_datasets: DataSetList) -> Asset:
    root_asset = AssetWrite(
        name="Toolkit Aggregators Test Root Asset",
        external_id="toolkit_aggregators_test_root_asset",
        data_set_id=aggregator_two_datasets[0].id,
    )
    retrieved = toolkit_client.assets.retrieve(external_id=root_asset.external_id)
    if retrieved is None:
        return toolkit_client.assets.create(root_asset)
    return retrieved


def create_raw_table_with_data(client: ToolkitClient, table: RawTable, rows: list[RowWrite]) -> None:
    loader = RawTableCRUD.create_loader(client)
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


@pytest.mark.usefixtures("aggregator_two_datasets")
@pytest.fixture(scope="session")
def aggregator_assets(
    toolkit_client: ToolkitClient, aggregator_raw_db: str, aggregator_root_asset: Asset
) -> Transformation:
    table_name = ASSET_TABLE
    rows = [
        RowWrite(
            key=f"asset_00{i}",
            columns={
                "name": f"Asset 00{i}",
                "externalId": f"asset_00{i}",
                "parentExternalId": aggregator_root_asset.external_id,
            },
        )
        for i in range(1, ASSET_COUNT - 1 + 1)  # -1 for root asset, +1 for inclusive range
    ]
    create_raw_table_with_data(
        toolkit_client,
        RawTable(db_name=aggregator_raw_db, table_name=table_name),
        rows,
    )

    transformation = TransformationWrite(
        external_id=ASSET_TRANSFORMATION,
        name="Toolkit Aggregators Test Asset Transformation",
        destination=TransformationDestination("assets"),
        query=f"""SELECT name as name, externalId as externalId, dataset_id('{ASSET_DATASET}') as dataSetId, parentExternalId as parentExternalId
FROM `{aggregator_raw_db}`.`{table_name}`""",
        ignore_null_fields=True,
    )
    created = upsert_transformation_with_run(toolkit_client, transformation)
    return created


@pytest.mark.usefixtures("aggregator_assets")
@pytest.fixture(scope="session")
def aggregator_asset_list(toolkit_client: ToolkitClient, aggregator_root_asset: Asset) -> AssetList:
    return toolkit_client.assets.list(
        asset_subtree_ids=[aggregator_root_asset.id],
    )


@pytest.mark.usefixtures("aggregator_two_datasets")
@pytest.fixture(scope="session")
def aggregator_events(
    toolkit_client: ToolkitClient, aggregator_raw_db: str, aggregator_asset_list: AssetList
) -> Transformation:
    table_name = EVENT_TABLE
    assets = aggregator_asset_list
    rows = [
        RowWrite(
            key=f"event_00{i}",
            columns={
                "externalId": f"event_00{i}",
                "name": f"Event 00{i}",
                "startTime": 1000000000 + i * 1000,  # Staggered start times
                "endTime": 1000000000 + i * 1000 + 500,  # 500ms duration
                "assetIds": [assets[i % len(assets)].id],
            },
        )
        for i in range(1, EVENT_COUNT + 1)  # +1 for inclusive range
    ]
    create_raw_table_with_data(
        toolkit_client,
        RawTable(db_name=aggregator_raw_db, table_name=table_name),
        rows,
    )
    transformation = TransformationWrite(
        external_id=EVENT_TRANSFORMATION,
        name="Toolkit Aggregators Test Event Transformation",
        destination=TransformationDestination("events"),
        query=f"""SELECT externalId as externalId, name as name, timestamp_millis(startTime) as startTime, timestamp_millis(endTime) as endTime,
assetIds as assetIds, dataset_id('{EVENT_DATASET}') as dataSetId
FROM `{aggregator_raw_db}`.`{table_name}`""",
        ignore_null_fields=True,
    )
    created = upsert_transformation_with_run(toolkit_client, transformation)
    return created


@pytest.fixture(scope="session")
def aggregator_files(
    toolkit_client: ToolkitClient,
    aggregator_raw_db: str,
    aggregator_two_datasets: DataSetList,
    aggregator_asset_list: AssetList,
) -> Transformation:
    table_name = FILE_TABLE
    assets = aggregator_asset_list
    rows = [
        RowWrite(
            key=f"file_00{i}",
            columns={
                "externalId": f"file_00{i}",
                "name": f"File 00{i}",
                "assetIds": [assets[i % len(assets)].id],  # Assign to one of the assets
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
        RawTable(db_name=aggregator_raw_db, table_name=table_name),
        rows,
    )
    transformation = TransformationWrite(
        external_id=FILE_TRANSFORMATION,
        name="Toolkit Aggregators Test File Transformation",
        destination=TransformationDestination("files"),
        query=f"""SELECT externalId as externalId, name as name, assetIds as assetIds,
dataset_id('{FILE_DATASET}') as dataSetId, mimeType as mimeType
FROM `{aggregator_raw_db}`.`{table_name}`""",
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
def aggregator_time_series(
    toolkit_client: ToolkitClient,
    aggregator_raw_db: str,
    aggregator_two_datasets: DataSetList,
    aggregator_asset_list: AssetList,
) -> Transformation:
    table_name = TIMESERIES_TABLE
    assets = aggregator_asset_list
    rows = [
        RowWrite(
            key=f"timeseries_00{i}",
            columns={
                "externalId": f"timeseries_00{i}",
                "name": f"Time Series 00{i}",
                "assetId": assets[i % len(assets)].id,  # Assign to one of the assets
                "isString": False,
                "isStep": False,
            },
        )
        for i in range(1, TIMESERIES_COUNT + 1)  # +1 for inclusive range
    ]
    create_raw_table_with_data(
        toolkit_client,
        RawTable(db_name=aggregator_raw_db, table_name=table_name),
        rows,
    )
    transformation = TransformationWrite(
        external_id=TIMESERIES_TRANSFORMATION,
        name="Toolkit Aggregators Test Time Series Transformation",
        destination=TransformationDestination("timeseries"),
        query=f"""SELECT externalId as externalId, name as name, assetId as assetId, isString as isString, isStep as isStep,
dataset_id('{TIMESERIES_DATASET}') as dataSetId
FROM `{aggregator_raw_db}`.`{table_name}`""",
        ignore_null_fields=True,
    )
    created = upsert_transformation_with_run(toolkit_client, transformation)
    return created


@pytest.fixture(scope="session")
def aggregator_sequences(
    toolkit_client: ToolkitClient,
    aggregator_raw_db: str,
    aggregator_two_datasets: DataSetList,
    aggregator_asset_list: AssetList,
) -> Transformation:
    table_name = SEQUENCE_TABLE
    assets = aggregator_asset_list
    rows = [
        RowWrite(
            key=f"sequence_00{i}",
            columns={
                "externalId": f"sequence_00{i}",
                "name": f"Sequence 00{i}",
                "assetId": assets[i % len(assets)].id,  # Assign to one of the assets
                "columns": [
                    {"name": f"Column {j}", "valueType": "STRING", "externalId": f"column_{j}"} for j in range(1, 4)
                ],
            },
        )
        for i in range(1, SEQUENCE_COUNT + 1)  # +1 for inclusive range
    ]
    create_raw_table_with_data(
        toolkit_client,
        RawTable(db_name=aggregator_raw_db, table_name=table_name),
        rows,
    )
    transformation = TransformationWrite(
        external_id=SEQUENCE_TRANSFORMATION,
        name="Toolkit Aggregators Test Sequence Transformation",
        destination=TransformationDestination("sequences"),
        query=f"""SELECT externalId as externalId, name as name, assetId as assetId, columns as columns, dataset_id('{SEQUENCE_DATASET}') as dataSetId
FROM `{aggregator_raw_db}`.`{table_name}`""",
        ignore_null_fields=True,
    )
    created = upsert_transformation_with_run(toolkit_client, transformation)
    return created


@pytest.fixture()
def disable_throttler(
    toolkit_client: ToolkitClient,
) -> None:
    def no_op(*args, **kwargs) -> None:
        """No operation function to replace the write_last_call_epoc function."""
        pass

    always_enabled = MagicMock(spec=ThrottlerState)
    # We mock the TrottlerState the mock object will always pass the throttling check.
    always_enabled.get.return_value = MagicMock(spec=ThrottlerState)
    with (
        patch(f"{raw_row_count.__module__}.ThrottlerState", always_enabled),
    ):
        yield
