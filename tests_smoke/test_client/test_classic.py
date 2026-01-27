from cognite.client.data_classes import DataSet

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataRequest
from tests_smoke.exceptions import EndpointAssertionError


class TestFileMetadataAPI:
    def test_create_retrieve_update_delete(self, smoke_dataset: DataSet, toolkit_client: ToolkitClient) -> None:
        request = FileMetadataRequest(
            external_id="smoke_test_file_metadata_external_id",
            name="Smoke Test File Metadata",
            data_set_id=smoke_dataset.id,
            mime_type="application/octet-stream",
            source="smoke_test_source",
        )
        try:
            created = toolkit_client.tool.filemetadata.create([request])
            if len(created) != 1:
                raise EndpointAssertionError(
                    "/files",
                    f"Expected 1 created filemedata, got {len(created)}",
                )
            if created[0].external_id != request.external_id:
                raise EndpointAssertionError(
                    "/files",
                    f"Expected created filemetadata external ID to be {request.external_id}, got {created[0].external_id}",
                )

            retrieved = toolkit_client.tool.filemetadata.retrieve([request.as_id()])
            if len(retrieved) != 1:
                raise EndpointAssertionError(
                    "/files/byids",
                    f"Expected 1 retrieved filemetadata, got {len(retrieved)}",
                )
            if retrieved[0].external_id != request.external_id:
                raise EndpointAssertionError(
                    "/files/byids",
                    f"Expected retrieved filemetadata external ID to be {request.external_id}, got {retrieved[0].external_id}",
                )

            update_request = created[0].as_request_resource().model_copy(update={"source": "Updated source"})
            updated = toolkit_client.tool.filemetadata.update([update_request])
            if len(updated) != 1:
                raise EndpointAssertionError(
                    "/files/update",
                    f"Expected 1 updated filemetadata, got {len(updated)}",
                )
            if updated[0].source != "Updated source":
                raise EndpointAssertionError(
                    "/files/update",
                    f"Expected updated filemetadata source to be 'Updated source', got {updated[0].source}",
                )
        finally:
            toolkit_client.tool.filemetadata.delete([request.as_id()])
