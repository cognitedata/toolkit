from collections.abc import Iterator
from pathlib import Path

import pytest
from cognite.client import data_modeling as dm
from cognite.client.data_classes import (
    AssetList,
    AssetWrite,
    AssetWriteList,
)
from cognite.client.data_classes.data_modeling import Space
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteAsset
from cognite.client.exceptions import CogniteAPIError

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.cdf_client import ResponseItems
from cognite_toolkit._cdf_tk.client.http_client import RequestMessage
from cognite_toolkit._cdf_tk.client.resource_classes.cognite_file import CogniteFileRequest
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataResponse
from cognite_toolkit._cdf_tk.commands._migrate.command import MigrationCommand
from cognite_toolkit._cdf_tk.commands._migrate.data_mapper import AssetCentricMapper
from cognite_toolkit._cdf_tk.commands._migrate.default_mappings import (
    ASSET_ID,
    EVENT_ID,
    FILE_METADATA_ID,
    TIME_SERIES_ID,
)
from cognite_toolkit._cdf_tk.commands._migrate.migration_io import (
    AnnotationMigrationIO,
    AssetCentricMigrationIO,
)
from cognite_toolkit._cdf_tk.commands._migrate.selectors import MigrateDataSetSelector, MigrationCSVFileSelector
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
            client.data_modeling.instances.delete([dm.NodeId(space, ts.external_id) for ts in output])
    created = client.assets.create(assets)

    yield created

    # Cleanup after test
    _ = client.data_modeling.instances.delete([dm.NodeId(space, ts.external_id) for ts in created])
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
            selectors=[MigrationCSVFileSelector(datafile=input_file, kind="Assets")],
            data=AssetCentricMigrationIO(client),
            mapper=AssetCentricMapper(client),
            log_dir=tmp_path / "logs",
            dry_run=False,
            verbose=False,
        )
        node_ids = [dm.NodeId(space, a.external_id) for a in three_assets]
        migrated_assets = client.data_modeling.instances.retrieve_nodes(node_ids, CogniteAsset)
        assert len(migrated_assets) == len(three_assets), "Not all assets were migrated successfully."

    def test_migrate_assets_by_dataset_dry_run(
        self, toolkit_client: ToolkitClient, migration_hierarchy_minimal: HierarchyMinimal, tmp_path: Path
    ) -> None:
        client = toolkit_client
        hierarchy = migration_hierarchy_minimal
        cmd = MigrationCommand(skip_tracking=True, silent=True)
        selector = MigrateDataSetSelector(
            kind="Assets",
            data_set_external_id=hierarchy.dataset.external_id,
            ingestion_mapping=ASSET_ID,
            preferred_consumer_view=ViewId(space="cdf_cdm", external_id="CogniteAsset", version="v1"),
        )
        result = cmd.migrate(
            selectors=[selector],
            data=AssetCentricMigrationIO(client),
            mapper=AssetCentricMapper(client),
            log_dir=tmp_path,
            dry_run=True,
        )
        results = {item.status: item.count for item in result[str(selector)]}
        assert results == {"failure": 0, "pending": 2, "success": 0, "unchanged": 0, "skipped": 0}


class TestMigrateEventsCommand:
    def test_migrate_events_by_dataset_dry_run(
        self, toolkit_client: ToolkitClient, migration_hierarchy_minimal: HierarchyMinimal, tmp_path: Path
    ) -> None:
        client = toolkit_client
        hierarchy = migration_hierarchy_minimal
        cmd = MigrationCommand(skip_tracking=True, silent=True)
        selector = MigrateDataSetSelector(
            kind="Events",
            data_set_external_id=hierarchy.dataset.external_id,
            ingestion_mapping=EVENT_ID,
            preferred_consumer_view=ViewId(space="cdf_cdm", external_id="CogniteActivity", version="v1"),
        )
        result = cmd.migrate(
            selectors=[selector],
            data=AssetCentricMigrationIO(client),
            mapper=AssetCentricMapper(client),
            log_dir=tmp_path,
            dry_run=True,
        )
        results = {item.status: item.count for item in result[str(selector)]}
        assert results == {"failure": 0, "pending": 1, "success": 0, "unchanged": 0, "skipped": 0}


class TestMigrateTimeSeriesCommand:
    def test_migrate_time_series_by_dataset_dry_run(
        self, toolkit_client: ToolkitClient, migration_hierarchy_minimal: HierarchyMinimal, tmp_path: Path
    ) -> None:
        client = toolkit_client
        hierarchy = migration_hierarchy_minimal
        cmd = MigrationCommand(skip_tracking=True, silent=True)
        selector = MigrateDataSetSelector(
            kind="TimeSeries",
            data_set_external_id=hierarchy.dataset.external_id,
            ingestion_mapping=TIME_SERIES_ID,
            preferred_consumer_view=ViewId(space="cdf_cdm", external_id="CogniteTimeSeries", version="v1"),
        )
        result = cmd.migrate(
            selectors=[selector],
            data=AssetCentricMigrationIO(client, skip_linking=True),
            mapper=AssetCentricMapper(client),
            log_dir=tmp_path,
            dry_run=True,
        )
        results = {item.status: item.count for item in result[str(selector)]}
        assert results == {"failure": 0, "pending": 1, "success": 0, "unchanged": 0, "skipped": 0}


class TestMigrateFileMetadataCommand:
    def test_migrate_file_metadata_by_dataset_dry_run(
        self, toolkit_client: ToolkitClient, migration_hierarchy_minimal: HierarchyMinimal, tmp_path: Path
    ) -> None:
        client = toolkit_client
        hierarchy = migration_hierarchy_minimal
        cmd = MigrationCommand(skip_tracking=True, silent=True)
        selector = MigrateDataSetSelector(
            kind="FileMetadata",
            data_set_external_id=hierarchy.dataset.external_id,
            ingestion_mapping=FILE_METADATA_ID,
            preferred_consumer_view=ViewId(space="cdf_cdm", external_id="CogniteFile", version="v1"),
        )
        result = cmd.migrate(
            selectors=[selector],
            data=AssetCentricMigrationIO(client, skip_linking=False),
            mapper=AssetCentricMapper(client),
            log_dir=tmp_path,
            dry_run=True,
        )
        results = {item.status: item.count for item in result[str(selector)]}
        assert results == {"failure": 0, "pending": 1, "success": 0, "unchanged": 0, "skipped": 0}


class TestMigrateAnnotations:
    def test_migrate_annotations_dry_run(
        self, toolkit_client: ToolkitClient, tmp_path: Path, migration_hierarchy_minimal: HierarchyMinimal
    ) -> None:
        client = toolkit_client
        hierarchy = migration_hierarchy_minimal
        cmd = MigrationCommand(skip_tracking=True, silent=True)
        selector = MigrateDataSetSelector(
            kind="Annotations",
            data_set_external_id=hierarchy.dataset.external_id,
        )
        result = cmd.migrate(
            selectors=[selector],
            data=AnnotationMigrationIO(client, instance_space="my_annotations_space"),
            mapper=AssetCentricMapper(client),
            log_dir=tmp_path,
            dry_run=True,
            verbose=True,
        )
        results = {item.status: item.count for item in result[str(selector)]}
        assert results == {"failure": 0, "pending": 2, "success": 0, "unchanged": 0, "skipped": 0}


@pytest.fixture()
def cdm_file(
    toolkit_client: ToolkitClient,
    toolkit_space: Space,
) -> FileMetadataResponse:
    client = toolkit_client
    space = toolkit_space.space
    node = CogniteFileRequest(
        space=space,
        external_id="cdm_file_testing_migrate_linked_file",
        name="CDM file testing migrate linked file",
    )
    exiting = client.tool.cognite_files.retrieve([node.as_id()])
    instance_id = node.as_instance_id()
    if len(exiting) == 1 and exiting[0].is_uploaded:
        return client.tool.filemetadata.retrieve([instance_id])[0]

    _ = client.tool.cognite_files.create([node])

    file = client.tool.filemetadata.upload_file_link([node.as_instance_id()])[0]
    _ = client.tool.filemetadata.upload_content(
        b"This is the CDM file content", file.upload_url, mime_type="text/plain"
    )

    response = client.http_client.request_single_retries(
        RequestMessage(
            endpoint_url=client.config.create_api_url("/files/update"),
            method="POST",
            body_content={"items": [{**instance_id.dump(), "update": {"externalId": {"set": node.external_id}}}]},
        )
    ).get_success_or_raise()
    return ResponseItems[FileMetadataResponse].model_valide_json(response.body).items[0]


@pytest.fixture
def selected_cdm_file(cdm_file: FileMetadataResponse, toolkit_space: Space, tmp_path: Path) -> MigrationCSVFileSelector:
    space = toolkit_space.space

    input_file = tmp_path / "file_migration.csv"

    with input_file.open("w", encoding="utf-8") as f:
        f.write(
            "id,dataSetId,space,externalId\n"
            + "\n".join(f"{f.id},{f.data_set_id or ''},{space},{f.external_id}" for f in [cdm_file])
            + "\n"
        )
    return MigrationCSVFileSelector(datafile=input_file, kind="FileMetadata")


class TestMigrateFiles:
    def test_migrate_linked_file(
        self, toolkit_client: ToolkitClient, selected_cdm_file: MigrationCSVFileSelector, tmp_path: Path
    ) -> None:
        client = toolkit_client

        cmd = MigrationCommand(skip_tracking=True, silent=True)

        result = cmd.migrate(
            selectors=[selected_cdm_file],
            data=AssetCentricMigrationIO(client, skip_linking=False),
            mapper=AssetCentricMapper(client),
            log_dir=tmp_path,
            dry_run=False,
        )
        actual_result = {item.status: item.count for item in result[str(selected_cdm_file)]}

        assert actual_result == {"failure": 1, "pending": 0, "success": 0, "unchanged": 0, "skipped": 0}, (
            "Expected failure as the file is already a CDM file."
        )

    def test_skip_linked_file(
        self, toolkit_client: ToolkitClient, selected_cdm_file: MigrationCSVFileSelector, tmp_path: Path
    ) -> None:
        client = toolkit_client
        cmd = MigrationCommand(skip_tracking=True, silent=True)

        result = cmd.migrate(
            selectors=[selected_cdm_file],
            data=AssetCentricMigrationIO(client, skip_existing=True),
            mapper=AssetCentricMapper(client),
            log_dir=tmp_path,
            dry_run=False,
        )

        actual_result = {item.status: item.count for item in result[str(selected_cdm_file)]}

        assert actual_result == {"failure": 0, "pending": 0, "success": 0, "unchanged": 0, "skipped": 1}, (
            "File already exists."
        )
