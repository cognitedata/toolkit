from pathlib import Path

from cognite.client.data_classes import FileMetadataWrite

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.storageio import FileContentIO
from cognite_toolkit._cdf_tk.storageio._file_content import UploadFileContentItem
from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient
from tests.test_integration.constants import RUN_UNIQUE_ID


class TestFileContentIO:
    def test_upload_file(self, toolkit_client: ToolkitClient, tmp_path: Path) -> None:
        my_text_file = tmp_path / "my_file.txt"
        my_text_file.write_text("This is some test content.", encoding="utf-8")
        metadata = FileMetadataWrite(
            name="my_file.txt",
            external_id=f"test_upload_file_001_{RUN_UNIQUE_ID}",
            source="TestUpload",
        )

        io = FileContentIO(toolkit_client)
        try:
            with HTTPClient(toolkit_client.config) as http_client:
                io.upload_items(
                    [
                        UploadFileContentItem(
                            source_id=my_text_file.name,
                            item=metadata,
                            file_path=my_text_file,
                            mime_type="text/plain",
                        )
                    ],
                    http_client,
                )
            # Verify upload
            uploaded_file = toolkit_client.files.retrieve(external_id=metadata.external_id)
            assert uploaded_file is not None
            assert uploaded_file.name == "my_file.txt"
            assert uploaded_file.uploaded is True
        finally:
            # Clean up
            toolkit_client.files.delete(external_id=metadata.external_id, ignore_unknown_ids=True)
