import tempfile
import time
from pathlib import Path

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, NodeId
from cognite_toolkit._cdf_tk.client.resource_classes.cognite_file import CogniteFileRequest
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataRequest, FileMetadataResponse
from cognite_toolkit._cdf_tk.client.resource_classes.pending_instance_id import PendingInstanceId
from tests.test_integration.constants import RUN_UNIQUE_ID


class TestExtendedFilesAPI:
    def test_set_pending_instance_id(self, dev_cluster_client: ToolkitClient, dev_space: str) -> None:
        """Happy path for setting a pending instance ID on a file.

        1. Create file with content.
        2. Set pending instance ID.
        3. Create a CogniteFile.
        4. Retrieve file content using the node ID.
        """
        client = dev_cluster_client
        external_id = f"ts_toolkit_integration_test_happy_path_files_{RUN_UNIQUE_ID}"
        metadata = FileMetadataRequest(
            external_id=external_id,
            name="Toolkit Integration Test Happy Path Files",
            mime_type="text/plain",
        )
        cognite_file = CogniteFileRequest(
            space=dev_space,
            external_id=external_id,
            name="Toolkit Integration Test Happy Path",
            mime_type="text/plain",
        )
        content = b"Hello, this is a test file's content."
        created: FileMetadataResponse | None = None
        created_dm = False
        try:
            created_files = client.tool.filemetadata.create([metadata])
            assert len(created_files) == 1
            created = created_files[0]
            assert created.upload_url is not None
            client.tool.filemetadata.upload_file(content, created.upload_url, created.mime_type)

            node_ref = NodeId(space=dev_space, external_id=external_id)
            updated = client.tool.filemetadata.set_pending_ids(
                [PendingInstanceId(pending_instance_id=node_ref, id=created.id)]
            )
            assert len(updated) == 1
            assert updated[0].pending_instance_id == node_ref

            client.tool.cognite_files.create([cognite_file], replace=True)
            created_dm = True

            linked: FileMetadataResponse | None = None
            for _ in range(30):
                retrieved = client.tool.filemetadata.retrieve([cognite_file.as_instance_id()], ignore_unknown_ids=True)
                linked = retrieved[0] if retrieved else None
                if linked is not None:
                    break
                time.sleep(1)
            assert linked is not None, "File metadata was not linked to the data model instance."
            assert linked.id == created.id

            downloads = client.tool.filemetadata.get_download_url([linked.as_internal_id()])
            assert downloads[0].download_url is not None
            with tempfile.TemporaryDirectory() as tmp:
                destination = Path(tmp) / "file.txt"
                client.tool.filemetadata.download_file(downloads[0].download_url, destination)
                assert destination.read_bytes() == content
        finally:
            if created is not None and not created_dm:
                client.tool.filemetadata.delete([ExternalId(external_id=external_id)], ignore_unknown_ids=True)
            if created_dm:
                client.tool.cognite_files.delete([cognite_file.as_id()])
