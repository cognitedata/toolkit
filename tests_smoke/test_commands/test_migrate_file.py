from collections.abc import Iterator
from pathlib import Path

import pytest
from cognite.client.data_classes import DataSet
from cognite.client.data_classes import data_modeling as dm
from cognite.client.data_classes.data_modeling import Space

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import RequestMessage, SuccessResponse, ToolkitAPIError
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataRequest, FileMetadataResponse
from cognite_toolkit._cdf_tk.commands import MigrationCommand
from cognite_toolkit._cdf_tk.commands._migrate.data_mapper import AssetCentricMapper
from cognite_toolkit._cdf_tk.commands._migrate.migration_io import AssetCentricMigrationIO
from cognite_toolkit._cdf_tk.commands._migrate.selectors import MigrationCSVFileSelector
from tests_smoke.exceptions import EndpointAssertionError


@pytest.fixture()
def classic_file_with_content(
    toolkit_client: ToolkitClient, smoke_dataset: DataSet, smoke_space: Space
) -> Iterator[FileMetadataResponse]:
    client = toolkit_client
    mime_type = "text/plain"
    external_id = "toolkit_classic_file_with_content_migration_smoke_test"
    metadata = FileMetadataRequest(
        external_id=external_id,
        name="Toolkit Classic File With Content Migration Smoke Test.txt",
        data_set_id=smoke_dataset.id,
        mime_type=mime_type,
    )
    # Ensure clean state
    client.data_modeling.instances.delete((smoke_space.space, external_id))
    try:
        client.tool.filemetadata.delete([metadata.as_id()], ignore_unknown_ids=True)
    except ToolkitAPIError as e:
        if (
            "Files scheduled for migration to data modeling with pending instance ids set cannot be deleted"
            in e.message
        ):
            # If the file is already scheduled for migration, retrieve and yield it. This means this test
            # was aborted previously after file creation but before cleanup.
            yield client.tool.filemetadata.retrieve([metadata.as_id()])[0]
            return

    created = client.tool.filemetadata.create([metadata])
    if len(created) != 1:
        raise EndpointAssertionError(
            client.tool.filemetadata._method_endpoint_map["create"].path,
            "Failed to create classic file metadata for migration test.",
        )
    created_file = created[0]
    if created_file.upload_url is None:
        raise AssertionError("Created classic file metadata has no upload URL.")
    response = client.http_client.request_single_retries(
        RequestMessage(
            endpoint_url=created_file.upload_url,
            method="PUT",
            content_type=mime_type,
            data_content=b"Toolkit classic file content for migration smoke test.",
        )
    )
    if not isinstance(response, SuccessResponse):
        raise EndpointAssertionError(
            created_file.upload_url,
            f"Failed to upload content for classic file metadata. Response: {response}",
        )

    yield created_file

    # Cleanup
    client.data_modeling.instances.delete((smoke_space.space, external_id))
    try:
        client.tool.filemetadata.delete([created_file.as_id()], ignore_unknown_ids=True)
    except ToolkitAPIError as e:
        if "files with instance ids must be deleted through data modeling" in str(e).lower():
            return
        raise


class TestMigrateFile:
    def test_migrate_file(
        self,
        classic_file_with_content: FileMetadataResponse,
        toolkit_client: ToolkitClient,
        smoke_dataset: DataSet,
        smoke_space: Space,
        tmp_path: Path,
    ) -> None:
        client = toolkit_client
        file = classic_file_with_content
        space = smoke_space.space
        input_file = tmp_path / "file_migration.csv"

        with input_file.open("w", encoding="utf-8") as f:
            f.write(
                "id,dataSetId,space,externalId\n"
                + "\n".join(f"{f.id},{f.data_set_id or ''},{space},{f.external_id}" for f in [file])
                + "\n"
            )

        cmd = MigrationCommand()
        cmd.migrate(
            selectors=[MigrationCSVFileSelector(kind="FileMetadata", datafile=input_file)],
            data=AssetCentricMigrationIO(client, skip_linking=False),
            mapper=AssetCentricMapper(client),
            log_dir=tmp_path / "migration_logs",
            dry_run=False,
            verbose=False,
        )

        assert file.external_id is not None, "File external ID is None, cannot validate migration."
        # Validate that the file exists in data modeling and has content.
        # In addition, check that the instanceId is set on the file metadata in CDF.
        nodes = client.data_modeling.instances.retrieve((space, file.external_id)).nodes
        if len(nodes) != 1:
            raise EndpointAssertionError(
                client.data_modeling.instances._RESOURCE_PATH,
                "Migrated file instance not found in data modeling after migration.",
            )
        migrated_node = nodes[0]
        if migrated_node.external_id != file.external_id:
            raise AssertionError("Migrated file instance external ID does not match expected value.")
        content = client.files.download_bytes(instance_id=dm.NodeId(space, external_id=file.external_id))
        if content != b"Toolkit classic file content for migration smoke test.":
            raise AssertionError("Migrated file content does not match expected content.")
        migrated_file = client.tool.filemetadata.retrieve([file.as_id()])
        if len(migrated_file) != 1:
            raise EndpointAssertionError(
                client.tool.filemetadata._method_endpoint_map["retrieve"].path,
                "Failed to retrieve migrated file metadata from CDF after migration.",
            )
        if migrated_file[0].instance_id is None:
            raise AssertionError("Migrated file metadata has no instance ID after migration.")
