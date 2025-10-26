import random
import time
from collections.abc import Iterable
from pathlib import Path

import pytest
from cognite.client.data_classes import FileMetadataList, FileMetadataWrite
from cognite.client.data_classes.data_modeling import NodeId, Space
from cognite.client.exceptions import CogniteAPIError

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.extended_timeseries import ExtendedTimeSeriesList
from cognite_toolkit._cdf_tk.commands import MigrateFilesCommand
from cognite_toolkit._cdf_tk.commands._migrate.adapter import (
    FileMetaAdapter,
    MigrationCSVFileSelector,
)
from cognite_toolkit._cdf_tk.commands._migrate.command import MigrationCommand
from cognite_toolkit._cdf_tk.commands._migrate.data_mapper import AssetCentricMapper
from cognite_toolkit._cdf_tk.commands._migrate.default_mappings import FILE_METADATA_ID


@pytest.fixture()
def three_files_with_content(
    toolkit_client_with_pending_ids: ToolkitClient, toolkit_space: Space
) -> Iterable[FileMetadataList]:
    client = toolkit_client_with_pending_ids
    space = toolkit_space.space
    files = FileMetadataList([])
    for i in range(3):
        file = FileMetadataWrite(
            external_id=f"toolkit_test_migration_{i}_{random.randint(0, 10_000)!s}",
            name=f"toolkit_test_migration_{i}",
        )
        files.append(file)
    output = client.files.retrieve_multiple(external_ids=files.as_external_ids(), ignore_unknown_ids=True)
    if output:
        try:
            client.files.delete(external_id=output.as_external_ids(), ignore_unknown_ids=True)
        except CogniteAPIError:
            client.data_modeling.instances.delete([NodeId(space, ts.external_id) for ts in output])
    created = FileMetadataList([])
    for file in files:
        uploaded = client.files.upload_bytes(b"test content", **file.dump(camel_case=False))
        created.append(uploaded)

    yield created

    # Cleanup after test
    deleted = client.data_modeling.instances.delete([NodeId(space, file.external_id) for file in created])
    if deleted.nodes:
        return
    client.files.delete(external_id=created.as_external_ids(), ignore_unknown_ids=True)


class TestMigrateFilesCommand:
    # This tests uses instances.apply_fast() which uses up to 4 workers for writing instances,
    # when this is used in parallel with other tests that uses instances.apply() then we get 5 workers in total,
    # which will trigger a 429 error.
    @pytest.mark.skip(
        "This is not yet enabled in the staging cluster that Toolkit uses for testing. Only runs a dev cluster."
    )
    @pytest.mark.usefixtures("max_two_workers")
    def test_migrate_files_command(
        self,
        toolkit_client_with_pending_ids: ToolkitClient,
        three_files_with_content: ExtendedTimeSeriesList,
        toolkit_space: Space,
        tmp_path: Path,
    ) -> None:
        client = toolkit_client_with_pending_ids
        space = toolkit_space.space

        input_file = tmp_path / "files_migration.csv"
        with input_file.open("w", encoding="utf-8") as f:
            f.write(
                "id,dataSetId,space,externalId\n"
                + "\n".join(
                    f"{ts.id},{ts.data_set_id if ts.data_set_id else ''},{space},{ts.external_id}"
                    for ts in three_files_with_content
                )
                + "\n"
            )

        cmd = MigrateFilesCommand(skip_tracking=True, silent=True)
        cmd.migrate_files(
            client=client,
            mapping_file=input_file,
            dry_run=False,
            verbose=False,
            auto_yes=True,
        )
        # Wait for syncer
        time.sleep(5)

        migrated_files = client.files.retrieve_multiple(external_ids=three_files_with_content.as_external_ids())

        missing_node_id = [ts.external_id for ts in migrated_files if ts.instance_id is None]
        assert not missing_node_id, f"Some files are missing NodeId: {missing_node_id}"

        node_ids = [ts.instance_id for ts in migrated_files]
        for node_id in node_ids:
            content = client.files.download_bytes(instance_id=node_id)
            assert content == b"test content", f"Content of file {node_id} does not match expected content."

    def test_migrate_files_v2(
        self,
        toolkit_client_with_pending_ids: ToolkitClient,
        three_files_with_content: FileMetadataList,
        toolkit_space: Space,
        tmp_path: Path,
    ):
        client = toolkit_client_with_pending_ids
        space = toolkit_space.space

        input_file = tmp_path / "files_migration.csv"
        with input_file.open("w", encoding="utf-8") as f:
            f.write(
                "id,space,externalId,ingestionView\n"
                + "\n".join(f"{ts.id},{space},{ts.external_id},{FILE_METADATA_ID}" for ts in three_files_with_content)
                + "\n"
            )

        cmd = MigrationCommand(skip_tracking=True, silent=True)
        results = cmd.migrate(
            selected=MigrationCSVFileSelector(datafile=input_file, kind="file"),
            data=FileMetaAdapter(client),
            mapper=AssetCentricMapper(client),
            log_dir=tmp_path / "logs",
            dry_run=False,
            verbose=False,
        )
        actual_results = [results.get_progress(f"file_{item.id}") for item in three_files_with_content]
        expected_results = [
            {
                cmd.Steps.DOWNLOAD: "success",
                cmd.Steps.CONVERT: "success",
                cmd.Steps.UPLOAD: "success",
            }
            for _ in three_files_with_content
        ]
        assert actual_results == expected_results

        # Wait for syncer by polling
        for _ in range(12):  # Poll for up to 60 seconds
            migrated_files = client.files.retrieve_multiple(external_ids=three_files_with_content.as_external_ids())
            if all(f.instance_id is not None for f in migrated_files):
                break
            time.sleep(5)

        missing_node_id = [ts.external_id for ts in migrated_files if ts.instance_id is None]
        assert not missing_node_id, f"Some files are missing NodeId: {missing_node_id}"

        node_ids = [ts.instance_id for ts in migrated_files]
        for node_id in node_ids:
            content = client.files.download_bytes(instance_id=node_id)
            assert content == b"test content", f"Content of file {node_id} does not match expected content."
