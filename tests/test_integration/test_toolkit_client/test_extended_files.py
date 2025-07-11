from cognite.client.data_classes import FileMetadata, FileMetadataWrite
from cognite.client.data_classes.data_modeling import NodeApplyResultList
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteFileApply

from cognite_toolkit._cdf_tk.client import ToolkitClient


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
            external_id="ts_toolkit_integration_test_happy_path_files",
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
