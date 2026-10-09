import os
import shutil
import time
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from cognite.client import CogniteClient, global_config
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import (
    DataSet,
    DataSetWrite,
    Function,
    RowWrite,
    RowWriteList,
)
from cognite.client.data_classes.data_modeling import Space, SpaceApply
from dotenv import load_dotenv
from pydantic import JsonValue
from rich import print

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.http_client import HTTPResult, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, InternalId, RawDatabaseId, RawTableId
from cognite_toolkit._cdf_tk.client.request_classes.filters import AnnotationFilter
from cognite_toolkit._cdf_tk.client.resource_classes.annotation import (
    AnnotationRequest,
    AnnotationResponse,
    AssetLinkData,
    BoundingBox,
    FileLinkData,
)
from cognite_toolkit._cdf_tk.client.resource_classes.asset import AssetRequest, AssetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    ContainerPropertyDefinition,
    ContainerRequest,
    ContainerResponse,
    InstanceSource,
    NodeRequest,
    SpaceRequest,
    TextProperty,
)
from cognite_toolkit._cdf_tk.client.resource_classes.datapoints import (
    Datapoint,
    DatapointsRequest,
    LatestDatapointRequest,
)
from cognite_toolkit._cdf_tk.client.resource_classes.dataset import DataSetRequest, DataSetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.event import EventRequest, EventResponse
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataRequest, FileMetadataResponse
from cognite_toolkit._cdf_tk.client.resource_classes.function_schedule import (
    FunctionScheduleRequest,
    FunctionScheduleResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.raw import (
    RAWDatabaseRequest,
    RAWDatabaseResponse,
    RAWRowRequest,
    RAWTableRequest,
)
from cognite_toolkit._cdf_tk.client.resource_classes.streams import (
    StreamRequest,
    StreamRequestSettings,
    StreamResponse,
    StreamTemplate,
)
from cognite_toolkit._cdf_tk.client.resource_classes.timeseries import TimeSeriesRequest, TimeSeriesResponse
from cognite_toolkit._cdf_tk.client.resource_classes.transformation import (
    AssetCentricDataSource,
    NonceCredentials,
    TransformationRequest,
    TransformationResponse,
)
from cognite_toolkit._cdf_tk.commands._migrate.data_model import INSTANCE_SOURCE_VIEW_ID
from cognite_toolkit._cdf_tk.commands.auth import EnvironmentVariables
from cognite_toolkit._cdf_tk.resource_ios import RawDatabaseIO, RawTableIO
from cognite_toolkit._cdf_tk.utils.cdf import ThrottlerState, raw_row_count
from tests.constants import REPO_ROOT
from tests.data import THREE_D_He2_FBX_ZIP
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
    TOOLKIT_TEST_STREAM,
)

THIS_FOLDER = Path(__file__).resolve().parent
TMP_FOLDER = THIS_FOLDER / "tmp"


@pytest.fixture(scope="session")
def toolkit_client_config() -> ToolkitClientConfig:
    load_dotenv(REPO_ROOT / ".env", override=True)
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
def env_vars(toolkit_client: ToolkitClient) -> EnvironmentVariables:
    env_vars = EnvironmentVariables.create_from_environment()
    # Ensure we use the client above that has CLIENT NAME set to the test name
    env_vars._client = toolkit_client
    return env_vars


@pytest.fixture(scope="session")
def toolkit_space(cognite_client: CogniteClient) -> Space:
    return cognite_client.data_modeling.spaces.apply(SpaceApply(space="toolkit_test_space"))


@pytest.fixture(scope="session")
def toolkit_raw_database(toolkit_client: ToolkitClient) -> RAWDatabaseResponse:
    client = toolkit_client
    name = "toolkit_integration_test_db"
    for batch in client.tool.raw.databases.iterate(limit=None):
        for db in batch:
            if db.name == name:
                return db
    created = client.tool.raw.databases.create([RAWDatabaseRequest(name=name)])
    assert len(created) == 1
    return created[0]


@pytest.fixture(scope="session")
def toolkit_stream(toolkit_client: ToolkitClient) -> StreamResponse:
    """BasicLiveData stream, created if it doesn't exist."""
    retrieved = toolkit_client.streams.retrieve([ExternalId(external_id=TOOLKIT_TEST_STREAM)], ignore_unknown_ids=True)
    if retrieved:
        return retrieved[0]
    created = toolkit_client.streams.create(
        [
            StreamRequest(
                external_id=TOOLKIT_TEST_STREAM,
                settings=StreamRequestSettings(template=StreamTemplate(name="BasicLiveData")),
            )
        ]
    )
    return created[0]


@pytest.fixture(scope="session")
def toolkit_record_container(toolkit_client: ToolkitClient, toolkit_space: Space) -> ContainerResponse:
    """Record container for integration tests, created if it doesn't exist."""
    container = ContainerRequest(
        space=toolkit_space.space,
        external_id="toolkit_test_record_container",
        name="Toolkit Test Record Container",
        used_for="record",
        properties={
            "name": ContainerPropertyDefinition(type=TextProperty()),
        },
    )
    retrieved = toolkit_client.tool.containers.retrieve([container.as_id()])
    if retrieved:
        return retrieved[0]
    created = toolkit_client.tool.containers.create([container])
    assert created, "Failed to create record container"
    return created[0]


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
def build_dir() -> Iterator[Path]:
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
    return ToolkitClient(config)


@pytest.fixture(scope="session")
def dummy_function(cognite_client: CogniteClient) -> Function:
    external_id = "integration_test_function_dummy"

    if existing := cognite_client.functions.retrieve(external_id=external_id):
        return existing

    async def handle(
        client: CogniteClient | None = None,
        data: dict[str, object] | None = None,
        secrets: dict[str, str] | None = None,
        function_call_info: dict[str, object] | None = None,
    ) -> dict[str, object]:
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
def dummy_schedule(toolkit_client: ToolkitClient, dummy_function: Function) -> FunctionScheduleResponse:
    client = toolkit_client
    name = "integration_test_schedule_dummy"
    if existing_list := client.tool.functions.schedules.list(name=name):
        if len(existing_list) > 1:
            client.tool.functions.schedules.delete([existing.as_id() for existing in existing_list[1:]])
        schedule = existing_list[0]
    else:
        schedule = client.tool.functions.schedules.create(
            [
                FunctionScheduleRequest(
                    name=name,
                    cron_expression="0 7 * * MON",
                    description="Original description.",
                    function_external_id=dummy_function.external_id,
                )
            ]
        )[0]
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
def populated_raw_table(toolkit_client: ToolkitClient, raw_data: RowWriteList) -> RawTableId:
    db_name = "toolkit_test_db"
    table_name = "toolkit_test_profiling_table"
    existing_dbs = toolkit_client.tool.raw.databases.list(limit=None)
    existing_table_names: set[str] = set()
    if db_name in {db.name for db in existing_dbs}:
        existing_table_names = {
            table.name for table in toolkit_client.tool.raw.tables.list(db_name=db_name, limit=None)
        }
    if table_name not in existing_table_names:
        toolkit_client.tool.raw.tables.rows.create(
            [
                RAWRowRequest(db_name=db_name, table_name=table_name, key=row.key, columns=row.columns or {})
                for row in raw_data
            ],
            ensure_parent=True,
        )
    return RawTableId(db_name=db_name, name=table_name)


@pytest.fixture(scope="session")
def aggregator_raw_db(toolkit_client: ToolkitClient) -> str:
    loader = RawDatabaseIO.create_io(toolkit_client)
    db_name = "toolkit_aggregators_test_db"
    if not loader.retrieve([RawDatabaseId(name=db_name)]):
        loader.create([RAWDatabaseRequest(name=db_name)])
    return db_name


@pytest.fixture(scope="session")
def aggregator_two_datasets(toolkit_client: ToolkitClient) -> list[DataSetResponse]:
    datasets = [
        DataSetRequest(external_id="toolkit_aggregators_test_dataset_1", name="Toolkit Aggregators Test Dataset 1"),
        DataSetRequest(external_id="toolkit_aggregators_test_dataset_2", name="Toolkit Aggregators Test Dataset 2"),
    ]
    retrieved = toolkit_client.tool.datasets.retrieve(
        [dataset.as_id() for dataset in datasets], ignore_unknown_ids=True
    )
    if not retrieved:
        return toolkit_client.tool.datasets.create(datasets)

    return retrieved


@pytest.fixture(scope="session")
def aggregator_root_asset(
    toolkit_client: ToolkitClient, aggregator_two_datasets: list[DataSetResponse]
) -> AssetResponse:
    root_asset = AssetRequest(
        name="Toolkit Aggregators Test Root Asset",
        external_id="toolkit_aggregators_test_root_asset",
        data_set_id=aggregator_two_datasets[0].id,
    )
    retrieved = toolkit_client.tool.assets.retrieve([root_asset.as_id()], ignore_unknown_ids=True)
    if not retrieved:
        return toolkit_client.tool.assets.create([root_asset])[0]
    return retrieved[0]


def _upload_file_content(
    client: ToolkitClient, external_id: str, content: str | bytes, mime_type: str | None = None
) -> FileMetadataResponse:
    links = client.tool.filemetadata.get_upload_url([ExternalId(external_id=external_id)])
    link = links[0] if links else None
    if link is None or link.upload_url is None:
        raise RuntimeError(f"No upload URL for file {external_id!r}.")
    client.tool.filemetadata.upload_file(content, link.upload_url, mime_type or link.mime_type)
    uploaded = client.tool.filemetadata.retrieve([ExternalId(external_id=external_id)])
    if not uploaded:
        raise RuntimeError(f"File {external_id!r} was not found after upload.")
    return uploaded[0]


def create_raw_table_with_data(client: ToolkitClient, table: RAWTableRequest, rows: list[RowWrite]) -> None:
    loader = RawTableIO.create_io(client)
    existing_tables = loader.retrieve([table.as_id()])
    if not existing_tables:
        loader.create([table])
    data = client.tool.raw.tables.rows.list(table.db_name, table.name, limit=len(rows))
    if not data:
        client.tool.raw.tables.rows.create(
            [
                RAWRowRequest(db_name=table.db_name, table_name=table.name, key=row.key, columns=row.columns or {})
                for row in rows
            ]
        )


def upsert_transformation_with_run(
    toolkit_client: ToolkitClient, transformation: TransformationRequest
) -> TransformationResponse:
    retrieved = toolkit_client.tool.transformations.retrieve(
        [transformation.as_id()], ignore_unknown_ids=True, with_job_details=True
    )
    created = retrieved[0] if retrieved else toolkit_client.tool.transformations.create([transformation])[0]
    if created.last_finished_job is None:
        nonce = toolkit_client.sessions.create_one_shot_token_exchange_session()
        job = toolkit_client.tool.transformations.run(
            InternalId(id=created.id),
            nonce=NonceCredentials(
                session_id=nonce.id,
                nonce=nonce.nonce,
                cdf_project_name=toolkit_client.config.project,
            ),
        )
        deadline = time.time() + 600
        while job.status in {"Created", "Running"}:
            if time.time() > deadline:
                raise TimeoutError(f"Transformation job {job.id} did not finish.")
            time.sleep(1)
            job = toolkit_client.tool.transformations.jobs.retrieve([job.as_id()])[0]
        assert job.error is None
    return created


@pytest.fixture(scope="session")
def aggregator_assets(
    toolkit_client: ToolkitClient, aggregator_raw_db: str, aggregator_root_asset: AssetResponse
) -> TransformationResponse:
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
        RAWTableRequest(db_name=aggregator_raw_db, name=table_name),
        rows,
    )

    transformation = TransformationRequest(
        external_id=ASSET_TRANSFORMATION,
        name="Toolkit Aggregators Test Asset Transformation",
        destination=AssetCentricDataSource(type="assets"),
        query=f"""SELECT name as name, externalId as externalId, dataset_id('{ASSET_DATASET}') as dataSetId, parentExternalId as parentExternalId
FROM `{aggregator_raw_db}`.`{table_name}`""",
        ignore_null_fields=True,
    )
    created = upsert_transformation_with_run(toolkit_client, transformation)
    return created


@pytest.fixture(scope="session")
def aggregator_asset_list(
    toolkit_client: ToolkitClient,
    aggregator_root_asset: AssetResponse,
    aggregator_assets: TransformationResponse,
    aggregator_two_datasets: list[DataSetResponse],
) -> list[AssetResponse]:
    return toolkit_client.tool.assets.list(
        asset_subtree_ids=[aggregator_root_asset.id],
    )


@pytest.fixture(scope="session")
def aggregator_events(
    toolkit_client: ToolkitClient,
    aggregator_raw_db: str,
    aggregator_asset_list: list[AssetResponse],
    aggregator_two_datasets: list[DataSetResponse],
) -> TransformationResponse:
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
        RAWTableRequest(db_name=aggregator_raw_db, name=table_name),
        rows,
    )
    transformation = TransformationRequest(
        external_id=EVENT_TRANSFORMATION,
        name="Toolkit Aggregators Test Event Transformation",
        destination=AssetCentricDataSource(type="events"),
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
    aggregator_two_datasets: list[DataSetResponse],
    aggregator_asset_list: list[AssetResponse],
) -> TransformationResponse:
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
        RAWTableRequest(db_name=aggregator_raw_db, name=table_name),
        rows,
    )
    transformation = TransformationRequest(
        external_id=FILE_TRANSFORMATION,
        name="Toolkit Aggregators Test File Transformation",
        destination=AssetCentricDataSource(type="files"),
        query=f"""SELECT externalId as externalId, name as name, assetIds as assetIds,
dataset_id('{FILE_DATASET}') as dataSetId, mimeType as mimeType
FROM `{aggregator_raw_db}`.`{table_name}`""",
        ignore_null_fields=True,
    )
    created = upsert_transformation_with_run(toolkit_client, transformation)

    # Upload content for the files
    external_ids = [external_id for row in rows if isinstance(external_id := row.columns["externalId"], str)]
    filemetadata = toolkit_client.tool.filemetadata.retrieve(ExternalId.from_external_ids(external_ids))
    is_uploaded_by_external_id = {
        file.external_id: file.uploaded for file in filemetadata if file.external_id is not None
    }
    for external_id in external_ids:
        if is_uploaded_by_external_id.get(external_id):
            continue
        _upload_file_content(toolkit_client, external_id, f"Content of {external_id}")

    return created


@pytest.fixture(scope="session")
def aggregator_time_series(
    toolkit_client: ToolkitClient,
    aggregator_raw_db: str,
    aggregator_two_datasets: list[DataSetResponse],
    aggregator_asset_list: list[AssetResponse],
) -> TransformationResponse:
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
        RAWTableRequest(db_name=aggregator_raw_db, name=table_name),
        rows,
    )
    transformation = TransformationRequest(
        external_id=TIMESERIES_TRANSFORMATION,
        name="Toolkit Aggregators Test Time Series Transformation",
        destination=AssetCentricDataSource(type="timeseries"),
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
    aggregator_two_datasets: list[DataSetResponse],
    aggregator_asset_list: list[AssetResponse],
) -> TransformationResponse:
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
        RAWTableRequest(db_name=aggregator_raw_db, name=table_name),
        rows,
    )
    transformation = TransformationRequest(
        external_id=SEQUENCE_TRANSFORMATION,
        name="Toolkit Aggregators Test Sequence Transformation",
        destination=AssetCentricDataSource(type="sequences"),
        query=f"""SELECT externalId as externalId, name as name, assetId as assetId, columns as columns, dataset_id('{SEQUENCE_DATASET}') as dataSetId
FROM `{aggregator_raw_db}`.`{table_name}`""",
        ignore_null_fields=True,
    )
    created = upsert_transformation_with_run(toolkit_client, transformation)
    return created


@pytest.fixture()
def disable_throttler(
    toolkit_client: ToolkitClient,
) -> Iterator[None]:
    def no_op(*args, **kwargs) -> None:
        """No operation function to replace the write_last_call_epoc function."""

    always_enabled = MagicMock(spec=ThrottlerState)
    # We mock the TrottlerState the mock object will always pass the throttling check.
    always_enabled.get.return_value = MagicMock(spec=ThrottlerState)
    with (
        patch(f"{raw_row_count.__module__}.ThrottlerState", always_enabled),
    ):
        yield


@dataclass
class HierarchyMinimal:
    root_asset: AssetResponse
    child_asset: AssetResponse
    event: EventResponse
    file: FileMetadataResponse
    timeseries: TimeSeriesResponse
    dataset: DataSetResponse
    file_annotation: AnnotationResponse
    asset_annotation: AnnotationResponse


@pytest.fixture(scope="session")
def migration_hierarchy_minimal(toolkit_client: ToolkitClient) -> HierarchyMinimal:
    root = "migration_test_root_asset"
    child_external_id = "migration_test_child_asset_1"
    event_external_id = "migration_test_event"
    file_external_id = "migration_test_file"
    timeseries_external_id = "migration_test_timeseries"
    dataset_external_id = "migration_test_dataset"
    client = toolkit_client
    dataset_request = DataSetRequest(
        external_id=dataset_external_id,
        name="Migration Test DataSet",
        description="DataSet for migration integration tests",
    )
    retrieved_datasets = client.tool.datasets.retrieve([dataset_request.as_id()], ignore_unknown_ids=True)
    data_set = retrieved_datasets[0] if retrieved_datasets else client.tool.datasets.create([dataset_request])[0]
    asset_source = "ToolkitAsset"
    event_source = "ToolkitEvent"
    file_source = "ToolkitFile"
    assets = [
        AssetRequest(
            name="Migration Test Root Asset",
            external_id=root,
            description="Root asset for migration integration tests",
            data_set_id=data_set.id,
            source=asset_source,
        ),
        AssetRequest(
            name="Migration Test Child Asset 1",
            external_id=child_external_id,
            description="Child asset 1 for migration integration tests",
            parent_external_id=root,
            data_set_id=data_set.id,
            source=asset_source,
        ),
    ]
    existing_assets = {
        asset.external_id: asset
        for asset in client.tool.assets.retrieve([asset.as_id() for asset in assets], ignore_unknown_ids=True)
    }
    to_create = [asset for asset in assets if asset.external_id not in existing_assets]
    to_update = [asset for asset in assets if asset.external_id in existing_assets]
    upserted_assets = {
        asset.external_id: asset
        for asset in [
            *(client.tool.assets.create(to_create) if to_create else []),
            *(client.tool.assets.update(to_update, mode="replace") if to_update else []),
        ]
    }
    created_assets = [upserted_assets[asset.external_id] for asset in assets if asset.external_id is not None]
    child_asset = created_assets[1]
    event = EventRequest(
        external_id=event_external_id,
        data_set_id=data_set.id,
        start_time=1_600_000_000_000,
        end_time=1_600_000_000_500,
        type="WorkOrder",
        asset_ids=[child_asset.id],
        source=event_source,
    )
    existing_events = client.tool.events.retrieve([event.as_id()], ignore_unknown_ids=True)
    created_event = (
        client.tool.events.update([event], mode="replace")[0]
        if existing_events
        else client.tool.events.create([event])[0]
    )
    file = FileMetadataRequest(
        external_id=file_external_id,
        name="migration_test_file.txt",
        mime_type="text/plain",
        data_set_id=data_set.id,
        asset_ids=[child_asset.id],
        source=file_source,
    )
    retrieved_files = client.tool.filemetadata.retrieve([file.as_id()], ignore_unknown_ids=True)
    created_file = retrieved_files[0] if retrieved_files else client.tool.filemetadata.create([file], overwrite=True)[0]
    if not created_file.uploaded:
        if created_file.external_id is None:
            raise RuntimeError(f"File {file_external_id!r} is missing an external ID.")
        created_file = _upload_file_content(
            client, created_file.external_id, "This is a test file.", created_file.mime_type
        )

    timeseries = TimeSeriesRequest(
        name="Migration Test Time Series",
        external_id=timeseries_external_id,
        unit="C",
        is_step=False,
        is_string=False,
        asset_id=child_asset.id,
        data_set_id=data_set.id,
        unit_external_id="temperature:deg_c",
    )
    retrieved_timeseries = client.tool.timeseries.retrieve([timeseries.as_id()], ignore_unknown_ids=True)
    created_timeseries = (
        retrieved_timeseries[0] if retrieved_timeseries else client.tool.timeseries.create([timeseries])[0]
    )

    latest = client.tool.timeseries.datapoints.latest([LatestDatapointRequest(external_id=timeseries_external_id)])
    if not any(series.datapoints for series in latest):
        client.tool.timeseries.datapoints.create(
            [
                DatapointsRequest(
                    external_id=timeseries_external_id,
                    datapoints=[
                        Datapoint(timestamp=1_600_000_000_000, value=20.0),
                        Datapoint(timestamp=1_600_000_000_500, value=21.5),
                    ],
                )
            ]
        )
    text_region = BoundingBox(x_min=0, y_min=0, x_max=0.1, y_max=0.1)
    file_annotation = AnnotationRequest(
        annotation_type="diagrams.FileLink",
        data=FileLinkData(file_ref=InternalId(id=created_file.id), text_region=text_region),
        status="approved",
        creating_app="toolkit",
        creating_app_version="1.0.0",
        creating_user="doctrino",
        annotated_resource_type="file",
        annotated_resource_id=created_file.id,
    )

    asset_annotation = AnnotationRequest(
        annotation_type="diagrams.AssetLink",
        data=AssetLinkData(asset_ref=InternalId(id=child_asset.id), text_region=text_region),
        status="approved",
        creating_app="toolkit",
        creating_app_version="1.0.0",
        creating_user="doctrino",
        annotated_resource_type="file",
        annotated_resource_id=created_file.id,
    )

    existing_annotations = client.tool.annotations.list(
        filter=AnnotationFilter(
            annotated_resource_type="file",
            annotated_resource_ids=[InternalId(id=created_file.id)],
        )
    )
    created_file_annotation = next(
        (
            annotation
            for annotation in existing_annotations
            if annotation.annotation_type == file_annotation.annotation_type
        ),
        None,
    )
    if created_file_annotation is None:
        created_file_annotation = client.tool.annotations.create([file_annotation])[0]
    created_asset_annotation = next(
        (
            annotation
            for annotation in existing_annotations
            if annotation.annotation_type == asset_annotation.annotation_type
        ),
        None,
    )
    if created_asset_annotation is None:
        created_asset_annotation = client.tool.annotations.create([asset_annotation])[0]

    # Create destination space
    client.tool.spaces.create([SpaceRequest(space=dataset_external_id)])

    # Populate the InstanceSourceView such that Annotation can look up file and asset information during migration.
    linage_nodes = [
        NodeRequest(
            space=dataset_external_id,
            external_id=external_id,
            sources=[
                InstanceSource(
                    source=INSTANCE_SOURCE_VIEW_ID,
                    properties={
                        "resourceType": resource_type,
                        "id": resource.id,
                        "dataSetId": data_set.id,
                        "classicExternalId": external_id,
                    },
                )
            ],
        )
        for resource_type, resource, external_id in [
            ("asset", created_assets[0], root),
            ("asset", created_assets[1], child_external_id),
            ("event", created_event, event_external_id),
            ("file", created_file, file_external_id),
            ("timeseries", created_timeseries, timeseries_external_id),
        ]
    ]
    client.tool.instances.create(linage_nodes)

    return HierarchyMinimal(
        root_asset=created_assets[0],
        child_asset=child_asset,
        event=created_event,
        file=created_file,
        timeseries=created_timeseries,
        dataset=data_set,
        file_annotation=created_file_annotation,
        asset_annotation=created_asset_annotation,
    )


def test_migration_hierarchy(migration_hierarchy_minimal: HierarchyMinimal) -> None:
    assert True, "Fixture migration_hierarchy_minimal failed"


# This much match the simulatorExternalId in the SimulatorModel definition of the
# complete_org/complete_org_alpha test cases.
SIMULATOR_EXTERNAL_ID = "integration-test-simulator"


@pytest.fixture(scope="session")
def simulator(toolkit_client: ToolkitClient) -> str:
    """Toolkit does not support simulator creation yet, but we support
    simulator models. Thus, we need this fixture to ensure a simulator exists for
    the simulator model to reference.
    """
    http_client = toolkit_client.http_client
    config = toolkit_client.config
    # Check if simulator already exists
    list_response = http_client.request_single_retries(
        RequestMessage(
            endpoint_url=config.create_api_url("/simulators/list"),
            method="POST",
            body_content={"limit": 1000},
        )
    )
    if simulator_external_id := _parse_simulator_response(list_response):
        return simulator_external_id

    creation_response = http_client.request_single_retries(
        RequestMessage(
            endpoint_url=config.create_api_url("/simulators"),
            method="POST",
            body_content={"items": [SIMULATOR]},
        )
    )
    simulator_external_id = _parse_simulator_response(creation_response)
    assert simulator_external_id is not None
    return simulator_external_id


@pytest.fixture(scope="session")
def simulator_integration(simulator: str, toolkit_dataset: DataSet, toolkit_client: ToolkitClient) -> str:
    external_id = "integration-test-simulator-integration"
    simulator_integration = {
        "externalId": external_id,
        "simulatorExternalId": simulator,
        "heartbeat": 0,
        "dataSetId": toolkit_dataset.id,
        "connectorVersion": "1.0.0",
        "simulatorVersion": "1.0.0",
        "licenseStatus": "AVAILABLE",
        "licenseLastCheckedTime": 0,
        "connectorStatus": "IDLE",
        "connectorStatusUpdatedTime": 0,
    }

    http_client = toolkit_client.http_client
    config = toolkit_client.config

    # Check if simulator integration already exists
    request = RequestMessage(
        endpoint_url=config.create_api_url("simulators/integrations/list"),
        method="POST",
        body_content={"filter": {"simulatorExternalIds": [simulator]}, "limit": 1000},
    )
    list_response = http_client.request_single_retries(request)
    body = list_response.get_success_or_raise(request).body_json
    assert "items" in body
    items = body["items"]
    for item in items:
        if item["externalId"] == external_id:
            return external_id
    request = RequestMessage(
        endpoint_url=config.create_api_url("simulators/integrations"),
        method="POST",
        body_content={"items": [simulator_integration]},
    )
    creation_response = http_client.request_single_retries(request)
    body = creation_response.get_success_or_raise(request).body_json
    assert "items" in body
    items = body["items"]
    for item in items:
        if item["externalId"] == external_id:
            return external_id
    raise ValueError("Failed to create or retrieve simulator integration.")


@pytest.fixture(scope="session")
def three_d_file(toolkit_client: ToolkitClient, toolkit_dataset: DataSet) -> FileMetadataResponse:
    client = toolkit_client
    meta = FileMetadataRequest(
        name="he2.fbx",
        data_set_id=toolkit_dataset.id,
        external_id="my_simulator_model_revision_file",
        metadata={"source": "integration_test"},
        mime_type="application/octet-stream",
        source="3d-models",
    )
    retrieved = client.tool.filemetadata.retrieve([meta.as_id()], ignore_unknown_ids=True)
    read = retrieved[0] if retrieved else None
    if read and read.uploaded is True:
        return read
    if read is None:
        read = client.tool.filemetadata.create([meta])[0]
    with zipfile.ZipFile(THREE_D_He2_FBX_ZIP, mode="r") as zip_ref:
        file_data = zip_ref.read("he2.fbx")
        read = _upload_file_content(client, "my_simulator_model_revision_file", file_data, read.mime_type)
    assert read.uploaded is True
    return read


def _parse_simulator_response(response: HTTPResult) -> str | None:
    assert isinstance(response, SuccessResponse)
    assert "items" in response.body_json
    items = response.body_json["items"]
    return next((item["externalId"] for item in items if item["externalId"] == SIMULATOR_EXTERNAL_ID), None)


SIMULATOR: dict[str, JsonValue] = {
    "name": "Integration Test Simulator",
    "externalId": SIMULATOR_EXTERNAL_ID,
    "fileExtensionTypes": ["txt"],
    "modelTypes": [{"name": "Steady State", "key": "SteadyState"}],
    "modelDependencies": [
        {
            "fileExtensionTypes": ["txt", "xml"],
            "fields": [
                {"name": "fieldA", "label": "label fieldA", "info": "info fieldA"},
                {"name": "fieldB", "label": "label fieldB", "info": "info fieldB"},
            ],
        },
    ],
    "stepFields": [
        {
            "stepType": "get/set",
            "fields": [
                {
                    "name": "objectName",
                    "label": "Simulation Object Name",
                    "info": "Enter the name of the DWSIM object, i.e. Feed",
                },
                {
                    "name": "objectProperty",
                    "label": "Simulation Object Property",
                    "info": "Enter the property of the DWSIM object, i.e. Temperature",
                },
            ],
        },
        {
            "stepType": "command",
            "fields": [
                {
                    "name": "command",
                    "label": "Command",
                    "info": "Select a command",
                    "options": [{"label": "Solve Flowsheet", "value": "Solve"}],
                }
            ],
        },
    ],
    "unitQuantities": [
        {
            "name": "mass",
            "label": "Mass",
            "units": [{"label": "kg", "name": "kg"}, {"label": "g", "name": "g"}, {"label": "lb", "name": "lb"}],
        },
        {
            "name": "time",
            "label": "Time",
            "units": [{"label": "s", "name": "s"}, {"label": "min.", "name": "min."}, {"label": "h", "name": "h"}],
        },
        {
            "name": "accel",
            "label": "Acceleration",
            "units": [
                {"label": "m/s2", "name": "m/s2"},
                {"label": "cm/s2", "name": "cm/s2"},
                {"label": "ft/s2", "name": "ft/s2"},
            ],
        },
        {
            "name": "force",
            "label": "Force",
            "units": [
                {"label": "N", "name": "N"},
                {"label": "dyn", "name": "dyn"},
                {"label": "kgf", "name": "kgf"},
                {"label": "lbf", "name": "lbf"},
            ],
        },
        {
            "name": "volume",
            "label": "Volume",
            "units": [
                {"label": "m3", "name": "m3"},
                {"label": "cm3", "name": "cm3"},
                {"label": "L", "name": "L"},
                {"label": "ft3", "name": "ft3"},
                {"label": "bbl", "name": "bbl"},
                {"label": "gal[US]", "name": "gal[US]"},
                {"label": "gal[UK]", "name": "gal[UK]"},
            ],
        },
        {
            "name": "density",
            "label": "Density",
            "units": [
                {"label": "kg/m3", "name": "kg/m3"},
                {"label": "g/cm3", "name": "g/cm3"},
                {"label": "lbm/ft3", "name": "lbm/ft3"},
            ],
        },
        {
            "name": "diameter",
            "label": "Diameter",
            "units": [{"label": "mm", "name": "mm"}, {"label": "in", "name": "in"}],
        },
        {
            "name": "distance",
            "label": "Distance",
            "units": [{"label": "m", "name": "m"}, {"label": "ft", "name": "ft"}, {"label": "cm", "name": "cm"}],
        },
        {
            "name": "heatflow",
            "label": "Heat Flow",
            "units": [
                {"label": "kW", "name": "kW"},
                {"label": "kcal/h", "name": "kcal/h"},
                {"label": "BTU/h", "name": "BTU/h"},
                {"label": "BTU/s", "name": "BTU/s"},
                {"label": "cal/s", "name": "cal/s"},
                {"label": "HP", "name": "HP"},
                {"label": "kJ/h", "name": "kJ/h"},
                {"label": "kJ/d", "name": "kJ/d"},
                {"label": "MW", "name": "MW"},
                {"label": "W", "name": "W"},
                {"label": "BTU/d", "name": "BTU/d"},
                {"label": "MMBTU/d", "name": "MMBTU/d"},
                {"label": "MMBTU/h", "name": "MMBTU/h"},
                {"label": "kcal/s", "name": "kcal/s"},
                {"label": "kcal/h", "name": "kcal/h"},
                {"label": "kcal/d", "name": "kcal/d"},
            ],
        },
        {
            "name": "pressure",
            "label": "Pressure",
            "units": [
                {"label": "Pa", "name": "Pa"},
                {"label": "atm", "name": "atm"},
                {"label": "kgf/cm2", "name": "kgf/cm2"},
                {"label": "kgf/cm2g", "name": "kgf/cm2g"},
                {"label": "lbf/ft2", "name": "lbf/ft2"},
                {"label": "kPa", "name": "kPa"},
                {"label": "kPag", "name": "kPag"},
                {"label": "bar", "name": "bar"},
                {"label": "barg", "name": "barg"},
                {"label": "ftH2O", "name": "ftH2O"},
                {"label": "inH2O", "name": "inH2O"},
                {"label": "inHg", "name": "inHg"},
                {"label": "mbar", "name": "mbar"},
                {"label": "mH2O", "name": "mH2O"},
                {"label": "mmH2O", "name": "mmH2O"},
                {"label": "mmHg", "name": "mmHg"},
                {"label": "MPa", "name": "MPa"},
                {"label": "psi", "name": "psi"},
                {"label": "psig", "name": "psig"},
            ],
        },
        {
            "name": "velocity",
            "label": "Velocity",
            "units": [
                {"label": "m/s", "name": "m/s"},
                {"label": "cm/s", "name": "cm/s"},
                {"label": "mm/s", "name": "mm/s"},
                {"label": "km/h", "name": "km/h"},
                {"label": "ft/h", "name": "ft/h"},
                {"label": "ft/min", "name": "ft/min"},
                {"label": "ft/s", "name": "ft/s"},
                {"label": "in/s", "name": "in/s"},
            ],
        },
        {
            "name": "temperature",
            "label": "Temperature",
            "units": [
                {"label": "K", "name": "K"},
                {"label": "R", "name": "R"},
                {"label": "C", "name": "C"},
                {"label": "F", "name": "F"},
            ],
        },
        {
            "name": "volumetricFlow",
            "label": "Volumetric Flow",
            "units": [
                {"label": "m3/h", "name": "m3/h"},
                {"label": "cm3/s", "name": "cm3/s"},
                {"label": "L/h", "name": "L/h"},
                {"label": "L/min", "name": "L/min"},
                {"label": "L/s", "name": "L/s"},
                {"label": "ft3/h", "name": "ft3/h"},
                {"label": "ft3/min", "name": "ft3/min"},
                {"label": "ft3/s", "name": "ft3/s"},
                {"label": "gal[US]/h", "name": "gal[US]/h"},
                {"label": "gal[US]/min", "name": "gal[US]/min"},
                {"label": "gal[US]/s", "name": "gal[US]/s"},
                {"label": "gal[UK]/h", "name": "gal[UK]/h"},
                {"label": "gal[UK]/min", "name": "gal[UK]/min"},
                {"label": "gal[UK]/s", "name": "gal[UK]/s"},
            ],
        },
    ],
}
