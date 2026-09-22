import time

from cognite.client.data_classes import FileMetadata
from cognite.client.data_classes.data_modeling import NodeApplyResultList
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteFileApply

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.identifiers import InternalId, NodeId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import SpaceResponse
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataRequest, FileMetadataResponse
from cognite_toolkit._cdf_tk.client.resource_classes.pending_instance_id import PendingInstanceId
from tests_smoke.constants import SMOKE_FILE_UNLINK_EXTERNAL_ID, SMOKE_SPACE


class TestExtendedFilesAPI:
    def test_unlink_instance_ids(self, toolkit_client: ToolkitClient, smoke_space: SpaceResponse) -> None:
        """Verify file metadata can be unlinked from a data model instance after linking."""
        client = toolkit_client
        space = SMOKE_SPACE
        metadata = FileMetadataRequest(
            external_id=SMOKE_FILE_UNLINK_EXTERNAL_ID,
            name="Toolkit Smoke Test File Unlink",
            mime_type="text/plain",
        )
        cognite_file = CogniteFileApply(
            space=space,
            external_id=SMOKE_FILE_UNLINK_EXTERNAL_ID,
            name="Toolkit Smoke Test File Unlink",
        )
        content = b"Hello, this is a smoke test file's content."
        created: FileMetadataResponse | None = None
        created_dm: NodeApplyResultList | None = None
        try:
            created_files = client.tool.filemetadata.create([metadata])
            if len(created_files) != 1:
                raise AssertionError("Expected exactly one file metadata record to be created.")
            created = created_files[0]
            client.files.upload_content_bytes(content, external_id=created.external_id)

            updated = client.tool.filemetadata.set_pending_ids(
                [
                    PendingInstanceId(
                        pending_instance_id=NodeId(space=cognite_file.space, external_id=cognite_file.external_id),
                        id=created.id,
                    )
                ]
            )
            if len(updated) != 1:
                raise AssertionError("Expected exactly one file metadata record after setting pending instance id.")

            pending_instance_id = updated[0].pending_instance_id
            if pending_instance_id is None:
                raise AssertionError("Expected pending instance id to be set on file metadata.")
            pending = pending_instance_id.dump()
            if pending != {
                "space": cognite_file.space,
                "externalId": cognite_file.external_id,
                "instanceType": "node",
            }:
                raise AssertionError("Pending instance id on file metadata did not match the CogniteFile instance id.")

            created_dm = client.data_modeling.instances.apply(cognite_file).nodes

            retrieved_ts: FileMetadata | None = None
            for _ in range(60):
                retrieved_ts = client.files.retrieve(instance_id=cognite_file.as_id())
                if retrieved_ts is not None:
                    break
                time.sleep(1)

            if retrieved_ts is None:
                raise AssertionError("File metadata was not linked to the data model instance within the wait period.")
            if retrieved_ts.id != created.id:
                raise AssertionError("Linked file metadata id did not match the created asset-centric file id.")

            unlinked = client.tool.filemetadata.unlink_instance_ids([InternalId(id=created.id)])
            if len(unlinked) != 1 or unlinked[0].id != created.id:
                raise AssertionError("Unlinking file instance ids did not return the expected file metadata.")

            client.data_modeling.instances.delete(cognite_file.as_id())
            created_dm = None

            retrieved_ts = client.files.retrieve(external_id=metadata.external_id)
            if retrieved_ts is None or retrieved_ts.id != created.id:
                raise AssertionError(
                    "Asset-centric file metadata should remain after unlinking and deleting the data model instance."
                )
        finally:
            if created is not None and created_dm is None:
                client.files.delete(external_id=metadata.external_id, ignore_unknown_ids=True)
            if created_dm is not None:
                client.data_modeling.instances.delete(cognite_file.as_id())
