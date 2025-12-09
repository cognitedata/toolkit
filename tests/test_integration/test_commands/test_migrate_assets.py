import time
import zipfile
from collections.abc import Iterator
from pathlib import Path

import pytest
from cognite.client.data_classes import (
    AssetList,
    AssetWrite,
    AssetWriteList,
    DataSet,
    FileMetadata,
    FileMetadataWrite,
    ThreeDModelRevision,
    ThreeDModelRevisionWrite,
)
from cognite.client.data_classes.data_modeling import NodeApply, NodeId, NodeOrEdgeData, Space, ViewId
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteAsset
from cognite.client.exceptions import CogniteAPIError

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.three_d import ThreeDModelClassicRequest, ThreeDModelResponse
from cognite_toolkit._cdf_tk.commands._migrate.command import MigrationCommand
from cognite_toolkit._cdf_tk.commands._migrate.data_mapper import AssetCentricMapper, ThreeDMapper
from cognite_toolkit._cdf_tk.commands._migrate.data_model import COGNITE_MIGRATION_MODEL, SPACE_SOURCE_VIEW_ID
from cognite_toolkit._cdf_tk.commands._migrate.default_mappings import (
    ASSET_ID,
    EVENT_ID,
    FILE_METADATA_ID,
    TIME_SERIES_ID,
)
from cognite_toolkit._cdf_tk.commands._migrate.migration_io import (
    AnnotationMigrationIO,
    AssetCentricMigrationIO,
    ThreeDMigrationIO,
)
from cognite_toolkit._cdf_tk.commands._migrate.selectors import MigrateDataSetSelector, MigrationCSVFileSelector
from cognite_toolkit._cdf_tk.storageio import UploadItem
from cognite_toolkit._cdf_tk.utils.http_client import SuccessResponse
from tests.data import THREE_D_He2_FBX_ZIP
from tests.test_integration.conftest import HierarchyMinimal
from tests.test_integration.constants import RUN_UNIQUE_ID


@pytest.fixture()
def three_assets(toolkit_client: ToolkitClient, toolkit_space: Space) -> Iterator[AssetList]:
    client = toolkit_client
    space = toolkit_space.space
    assets = AssetWriteList([])
    for i in range(3):
        asset = AssetWrite(
            external_id=f"toolkit_asset_test_migration_{i}_{RUN_UNIQUE_ID}",
            name=f"toolkit_asset_test_migration_{i}_{RUN_UNIQUE_ID}",
            parent_external_id=f"toolkit_asset_test_migration_{0}_{RUN_UNIQUE_ID}" if i > 0 else None,
        )
        assets.append(asset)
    output = client.assets.retrieve_multiple(external_ids=assets.as_external_ids(), ignore_unknown_ids=True)
    if output:
        try:
            client.assets.delete(external_id=output.as_external_ids(), ignore_unknown_ids=True)
        except CogniteAPIError:
            client.data_modeling.instances.delete([NodeId(space, ts.external_id) for ts in output])
    created = client.assets.create(assets)

    yield created

    # Cleanup after test
    _ = client.data_modeling.instances.delete([NodeId(space, ts.external_id) for ts in created])
    client.assets.delete(external_id=created.as_external_ids(), ignore_unknown_ids=True, recursive=True)


class TestMigrateAssetsCommand:
    def test_migrate_assets(
        self,
        toolkit_client: ToolkitClient,
        three_assets: AssetList,
        toolkit_space: Space,
        tmp_path: Path,
    ) -> None:
        client = toolkit_client
        space = toolkit_space.space

        input_file = tmp_path / "timeseries_migration.csv"
        with input_file.open("w", encoding="utf-8") as f:
            f.write(
                "id,dataSetId,space,externalId\n"
                + "\n".join(
                    f"{a.id},{a.data_set_id if a.data_set_id else ''},{space},{a.external_id}" for a in three_assets
                )
                + "\n"
            )

        cmd = MigrationCommand(skip_tracking=True, silent=True)
        cmd.migrate(
            selected=MigrationCSVFileSelector(datafile=input_file, kind="Assets"),
            data=AssetCentricMigrationIO(client),
            mapper=AssetCentricMapper(client),
            log_dir=tmp_path / "logs",
            dry_run=False,
            verbose=False,
        )
        node_ids = [NodeId(space, a.external_id) for a in three_assets]
        migrated_assets = client.data_modeling.instances.retrieve_nodes(node_ids, CogniteAsset)
        assert len(migrated_assets) == len(three_assets), "Not all assets were migrated successfully."

    def test_migrate_assets_by_dataset_dry_run(
        self, toolkit_client: ToolkitClient, migration_hierarchy_minimal: HierarchyMinimal, tmp_path: Path
    ) -> None:
        client = toolkit_client
        hierarchy = migration_hierarchy_minimal
        cmd = MigrationCommand(skip_tracking=True, silent=True)
        progress = cmd.migrate(
            selected=MigrateDataSetSelector(
                kind="Assets",
                data_set_external_id=hierarchy.dataset.external_id,
                ingestion_mapping=ASSET_ID,
                preferred_consumer_view=ViewId("cdf_cdm", "CogniteAsset", "v1"),
            ),
            data=AssetCentricMigrationIO(client),
            mapper=AssetCentricMapper(client),
            log_dir=tmp_path,
            dry_run=True,
        )
        results = progress.aggregate()
        expected_results = {(step, "success"): 2 for step in cmd.Steps.list()}
        assert results == expected_results


class TestMigrateEventsCommand:
    def test_migrate_events_by_dataset_dry_run(
        self, toolkit_client: ToolkitClient, migration_hierarchy_minimal: HierarchyMinimal, tmp_path: Path
    ) -> None:
        client = toolkit_client
        hierarchy = migration_hierarchy_minimal
        cmd = MigrationCommand(skip_tracking=True, silent=True)
        progress = cmd.migrate(
            selected=MigrateDataSetSelector(
                kind="Events",
                data_set_external_id=hierarchy.dataset.external_id,
                ingestion_mapping=EVENT_ID,
                preferred_consumer_view=ViewId("cdf_cdm", "CogniteActivity", "v1"),
            ),
            data=AssetCentricMigrationIO(client),
            mapper=AssetCentricMapper(client),
            log_dir=tmp_path,
            dry_run=True,
        )
        results = progress.aggregate()
        expected_results = {(step, "success"): 1 for step in cmd.Steps.list()}
        assert results == expected_results


class TestMigrateTimeSeriesCommand:
    def test_migrate_time_series_by_dataset_dry_run(
        self, toolkit_client: ToolkitClient, migration_hierarchy_minimal: HierarchyMinimal, tmp_path: Path
    ) -> None:
        client = toolkit_client
        hierarchy = migration_hierarchy_minimal
        cmd = MigrationCommand(skip_tracking=True, silent=True)
        progress = cmd.migrate(
            selected=MigrateDataSetSelector(
                kind="TimeSeries",
                data_set_external_id=hierarchy.dataset.external_id,
                ingestion_mapping=TIME_SERIES_ID,
                preferred_consumer_view=ViewId("cdf_cdm", "CogniteTimeSeries", "v1"),
            ),
            data=AssetCentricMigrationIO(client, skip_linking=True),
            mapper=AssetCentricMapper(client),
            log_dir=tmp_path,
            dry_run=True,
        )
        results = progress.aggregate()
        expected_results = {(step, "success"): 1 for step in cmd.Steps.list()}
        assert results == expected_results


class TestMigrateFileMetadataCommand:
    def test_migrate_file_metadata_by_dataset_dry_run(
        self, toolkit_client: ToolkitClient, migration_hierarchy_minimal: HierarchyMinimal, tmp_path: Path
    ) -> None:
        client = toolkit_client
        hierarchy = migration_hierarchy_minimal
        cmd = MigrationCommand(skip_tracking=True, silent=True)
        progress = cmd.migrate(
            selected=MigrateDataSetSelector(
                kind="FileMetadata",
                data_set_external_id=hierarchy.dataset.external_id,
                ingestion_mapping=FILE_METADATA_ID,
                preferred_consumer_view=ViewId("cdf_cdm", "CogniteFile", "v1"),
            ),
            data=AssetCentricMigrationIO(client, skip_linking=False),
            mapper=AssetCentricMapper(client),
            log_dir=tmp_path,
            dry_run=True,
        )
        results = progress.aggregate()
        expected_results = {(step, "success"): 1 for step in cmd.Steps.list()}
        assert results == expected_results


class TestMigrateAnnotations:
    def test_migrate_annotations_dry_run(
        self, toolkit_client: ToolkitClient, tmp_path: Path, migration_hierarchy_minimal: HierarchyMinimal
    ) -> None:
        client = toolkit_client
        hierarchy = migration_hierarchy_minimal
        cmd = MigrationCommand(skip_tracking=True, silent=True)
        progress = cmd.migrate(
            selected=MigrateDataSetSelector(
                kind="Annotations",
                data_set_external_id=hierarchy.dataset.external_id,
            ),
            data=AnnotationMigrationIO(client, instance_space="my_annotations_space"),
            mapper=AssetCentricMapper(client),
            log_dir=tmp_path,
            dry_run=True,
            verbose=True,
        )
        results = progress.aggregate()
        expected_results = {(step, "success"): 2 for step in cmd.Steps.list()}
        assert results == expected_results


@pytest.fixture(scope="session")
def three_d_file(toolkit_client: ToolkitClient, toolkit_dataset: DataSet) -> FileMetadata:
    client = toolkit_client
    meta = FileMetadataWrite(
        name="he2.fbx",
        data_set_id=toolkit_dataset.id,
        external_id="toolkit_3d_model_test_file_external_id",
        metadata={"source": "integration_test"},
        mime_type="application/octet-stream",
        source="3d-models",
    )
    read = client.files.retrieve(external_id=meta.external_id)
    if read and read.uploaded is True:
        return read
    if read is None:
        read = client.files.create(meta)
    with zipfile.ZipFile(THREE_D_He2_FBX_ZIP, mode="r") as zip_ref:
        file_data = zip_ref.read("he2.fbx")
        read = client.files.upload_content_bytes(file_data, external_id=meta.external_id)
    assert read.uploaded is True
    return read


@pytest.fixture
def a_three_d_model(
    toolkit_client: ToolkitClient, three_d_file: FileMetadata, toolkit_dataset: DataSet
) -> Iterator[ThreeDModelResponse]:
    client = toolkit_client
    model_request = ThreeDModelClassicRequest(
        name=f"toolkit_3d_model_migration_test_{RUN_UNIQUE_ID}",
        data_set_id=toolkit_dataset.id,
        metadata={"source": "integration_test_migration"},
    )
    models = client.tool.three_d.models.create([model_request])
    assert len(models) == 1
    model = models[0]

    revision = client.three_d.revisions.create(
        model.id, ThreeDModelRevisionWrite(file_id=three_d_file.id, published=True)
    )
    assert isinstance(revision, ThreeDModelRevision)
    while revision.status in {"Processing", "Queued"}:
        revision = client.three_d.revisions.retrieve(model.id, revision.id)
        time.sleep(1)
    assert revision.status == "Done"
    models = client.tool.three_d.models.iterate(include_revision_info=True)
    retrieved_model = next((m for m in models if m.id == model.id), None)
    assert retrieved_model is not None
    yield retrieved_model
    client.tool.three_d.models.delete([model.id])


@pytest.fixture(scope="session")
def three_d_model_instance_space(toolkit_client: ToolkitClient, toolkit_space: Space, toolkit_dataset: DataSet) -> None:
    """This sets up the instance space mapping from the classic dataset."""
    client = toolkit_client
    space = toolkit_space.space
    client.data_modeling.instances.apply(
        NodeApply(
            space=COGNITE_MIGRATION_MODEL.space,
            external_id=space,
            sources=[
                NodeOrEdgeData(
                    source=SPACE_SOURCE_VIEW_ID,
                    properties={
                        "instanceSpace": space,
                        "dataSetId": toolkit_dataset.id,
                        "dataSetExternalId": toolkit_dataset.external_id,
                    },
                )
            ],
        )
    )


class TestMigrate3D:
    @pytest.mark.skip("This is an expensive test to run as it requires processing a 3D model in CDF.")
    @pytest.mark.usefixtures("three_d_model_instance_space")
    def test_migrate_3d_model(
        self, a_three_d_model: ThreeDModelResponse, toolkit_client: ToolkitClient, tmp_path: Path
    ) -> None:
        model = a_three_d_model

        mapper = ThreeDMapper(toolkit_client)

        mapped = mapper.map([model])
        assert len(mapped) == 1
        migration_request, issue = mapped[0]
        assert issue.has_issues is False
        io = ThreeDMigrationIO(toolkit_client)

        result = io.upload_items([UploadItem(source_id=str(model.id), item=migration_request)])

        failed = [res for res in result if not isinstance(res, SuccessResponse)]
        assert len(failed) == 0, f"Migration of 3D model failed with errors: {failed}"
