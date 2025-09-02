import time

from cognite.client.data_classes import FileMetadata, FileMetadataWrite
from cognite.client.data_classes.data_modeling import NodeApplyResultList, Space
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteFileApply

from cognite_toolkit._cdf_tk.client import ToolkitClient
from tests.test_integration.constants import RUN_UNIQUE_ID


class TestExtendedFilesAPI:
    def test_set_pending_instance_id(self, dev_cluster_client: ToolkitClient, dev_space: str) -> None:
        """Happy path for setting a pending instance ID on a file

        1. Create file with content.
        3. Set pending instance ID.
        4. Create a CogniteFile
        5. Retrieve file content using the Node ID.
        """
        client = dev_cluster_client
        metadata = FileMetadataWrite(
            external_id=f"ts_toolkit_integration_test_happy_path_files_{RUN_UNIQUE_ID}",
            name="Toolkit Integration Test Happy Path Files",
            mime_type="text/plain",
        )
        cognite_file = CogniteFileApply(
            space=dev_space,
            external_id=metadata.external_id,
            name="Toolkit Integration Test Happy Path",
            mime_type="text/plain",
        )
        content = b"Hello, this is a test file's content."
        created: FileMetadata | None = None
        created_dm: NodeApplyResultList | None = None
        try:
            created, _ = client.files.create(metadata)
            client.files.upload_content_bytes(content, external_id=created.external_id)

            updated = client.files.set_pending_ids(cognite_file.as_id(), id=created.id)
            assert updated.pending_instance_id == cognite_file.as_id()

            created_dm = client.data_modeling.instances.apply(cognite_file).nodes

            downloaded_bytes = client.files.download_bytes(instance_id=cognite_file.as_id())

            assert downloaded_bytes == content
        finally:
            if created is not None and created_dm is None:
                client.files.delete(external_id=metadata.external_id)
            if created_dm is not None:
                # This will delete the CogniteFile and the asset-centric file
                client.data_modeling.instances.delete(cognite_file.as_id())

    def test_unlink_instance_ids(self, toolkit_client_with_pending_ids: ToolkitClient, toolkit_space: Space) -> None:
        client = toolkit_client_with_pending_ids
        space = toolkit_space.space
        metadata = FileMetadataWrite(
            external_id=f"file_toolkit_integration_test_unlink_{RUN_UNIQUE_ID}",
            name="Toolkit Integration Test Unlink",
            mime_type="text/plain",
        )
        cognite_file = CogniteFileApply(
            space=space,
            external_id=metadata.external_id,
            name="Toolkit Integration Test Unlink",
        )
        content = b"Hello, this is a test file's content."
        created: FileMetadata | None = None
        created_dm: NodeApplyResultList | None = None
        try:
            created, _ = client.files.create(metadata)
            client.files.upload_content_bytes(content, external_id=created.external_id)

            updated = client.files.set_pending_ids(cognite_file.as_id(), id=created.id)

            assert updated.pending_instance_id == cognite_file.as_id()

            created_dm = client.data_modeling.instances.apply(cognite_file).nodes

            retrieved_ts: FileMetadata | None = None
            for _ in range(30):  # Wait up to 30 seconds for the syncer to update the file metadata
                retrieved_ts = client.files.retrieve(instance_id=cognite_file.as_id())
                if retrieved_ts is not None:
                    break
                time.sleep(1)  # Wait for the syncer to update the file metadata

            assert retrieved_ts is not None, "File was not linked to instance within timeout"
            assert retrieved_ts.id == created.id

            unlinked = client.files.unlink_instance_ids(id=created.id)
            assert unlinked.id == created.id

            client.data_modeling.instances.delete(cognite_file.as_id())
            created_dm = None

            # Still existing asset-centric file.
            retrieved_ts = client.files.retrieve(external_id=metadata.external_id)
            assert retrieved_ts is not None
            assert retrieved_ts.id == created.id
        finally:
            if created is not None and created_dm is None:
                client.files.delete(external_id=metadata.external_id, ignore_unknown_ids=True)
            if created_dm is not None:
                client.data_modeling.instances.delete(cognite_file.as_id())
