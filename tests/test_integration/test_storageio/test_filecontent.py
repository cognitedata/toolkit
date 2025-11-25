from pathlib import Path

from cognite.client.data_classes import FileMetadataWrite
from cognite.client.data_classes.data_modeling import NodeId, Space

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.storageio import FileContentIO
from cognite_toolkit._cdf_tk.storageio._file_content import UploadFileContentItem
from cognite_toolkit._cdf_tk.storageio.selectors import (
    FileDataModelingTemplate,
    FileDataModelingTemplateSelector,
    FileMetadataTemplate,
    FileMetadataTemplateSelector,
)
from cognite_toolkit._cdf_tk.storageio.selectors._file_content import FILENAME_VARIABLE
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
                    FileMetadataTemplateSelector(
                        file_directory=Path("does not matter"),
                        template=FileMetadataTemplate(name=FILENAME_VARIABLE, external_id=FILENAME_VARIABLE),
                    ),
                )
            # Verify upload
            uploaded_file = toolkit_client.files.retrieve(external_id=metadata.external_id)
            assert uploaded_file is not None
            assert uploaded_file.name == "my_file.txt"
            assert uploaded_file.uploaded is True
        finally:
            # Clean up
            toolkit_client.files.delete(external_id=metadata.external_id, ignore_unknown_ids=True)

    def test_upload_dm_file(self, toolkit_client: ToolkitClient, tmp_path: Path, toolkit_space: Space) -> None:
        my_text_file = tmp_path / "my_dm_file.txt"
        my_text_file.write_text("This is some test content for data modeling.", encoding="utf-8")
        metadata = FileMetadataWrite(
            name="my_dm_file.txt",
            instance_id=NodeId(
                space=toolkit_space.space,
                external_id=f"test_upload_dm_file_001_{RUN_UNIQUE_ID}",
            ),
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
                    FileDataModelingTemplateSelector(
                        file_directory=Path("does not matter"),
                        template=FileDataModelingTemplate(
                            space=toolkit_space.space,
                            external_id=FILENAME_VARIABLE,
                        ),
                    ),
                )
            # Verify upload
            uploaded_file = toolkit_client.files.retrieve(instance_id=metadata.instance_id)
            assert uploaded_file is not None
            assert uploaded_file.name == "my_dm_file.txt"
            assert uploaded_file.uploaded is True
        finally:
            # Clean up
            toolkit_client.data_modeling.instances.delete(nodes=[metadata.instance_id])
