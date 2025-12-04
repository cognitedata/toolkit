import time
from pathlib import Path

from cognite.client.data_classes import DataSet
from cognite.client.data_classes.data_modeling import NodeId, Space

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands import UploadCommand
from cognite_toolkit._cdf_tk.storageio import FileContentIO
from cognite_toolkit._cdf_tk.storageio.selectors import (
    FileDataModelingTemplate,
    FileDataModelingTemplateSelector,
    FileIdentifierSelector,
    FileMetadataTemplate,
    FileMetadataTemplateSelector,
)
from cognite_toolkit._cdf_tk.storageio.selectors._file_content import (
    FILENAME_VARIABLE,
    FILEPATH,
    FileExternalID,
    FileInstanceID,
    TemplateNodeId,
)
from cognite_toolkit._cdf_tk.utils.fileio import MultiFileReader, NDJsonWriter, Uncompressed
from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient
from tests.test_integration.constants import RUN_UNIQUE_ID


class TestFileContentIO:
    def test_upload_download_asset_centric(self, toolkit_client: ToolkitClient, tmp_path: Path) -> None:
        filename = "my_text_file.txt"
        my_text_file = tmp_path / "my_files" / filename
        external_id = f"{filename}_{RUN_UNIQUE_ID}"
        my_text_file.parent.mkdir(parents=True, exist_ok=True)
        my_text_file.write_text("This is some test content.", encoding="utf-8")
        directory = "/asset_centric"
        selector = FileMetadataTemplateSelector(
            file_directory=my_text_file.parent,
            template=FileMetadataTemplate.model_validate(
                dict(
                    name=FILENAME_VARIABLE,
                    external_id=f"{FILENAME_VARIABLE}_{RUN_UNIQUE_ID}",
                    directory=directory,
                    source="TestUpload",
                )
            ),
        )
        io = FileContentIO(toolkit_client, tmp_path)
        reader = MultiFileReader([my_text_file])

        read_chunks = list(io.read_chunks(reader, selector))
        assert len(read_chunks) == 1
        read_chunk = read_chunks[0]

        upload_content = io.json_chunk_to_data(read_chunk)
        assert len(upload_content) == 1
        try:
            with HTTPClient(toolkit_client.config) as http_client:
                io.upload_items(upload_content, http_client, selector)
            # Verify upload
            uploaded_file = toolkit_client.files.retrieve(external_id=external_id)
            assert uploaded_file is not None
            assert uploaded_file.name == filename
            assert uploaded_file.uploaded is True

            # Test download
            download_selector = FileIdentifierSelector(identifiers=(FileExternalID(external_id=external_id),))
            downloaded_files = [item for page in io.stream_data(download_selector) for item in page.items]
            assert len(downloaded_files) == 1
            expected_file = tmp_path / directory.removeprefix("/") / filename
            assert expected_file.is_file()
            downloaded_content = expected_file.read_text(encoding="utf-8")
            assert downloaded_content == "This is some test content."

            json_chunks = io.data_to_json_chunk(downloaded_files, download_selector)
            assert len(json_chunks) == 1
        finally:
            # Clean up
            toolkit_client.files.delete(external_id=external_id, ignore_unknown_ids=True)

    def test_upload_download_dm_file(self, toolkit_client: ToolkitClient, tmp_path: Path, toolkit_space: Space) -> None:
        external_id = f"test_upload_dm_file_001_{RUN_UNIQUE_ID}.txt"
        my_text_file = tmp_path / external_id
        my_text_file.write_text("This is some test content for data modeling.", encoding="utf-8")
        instance_id = NodeId(space=toolkit_space.space, external_id=external_id)
        selector = FileDataModelingTemplateSelector(
            file_directory=tmp_path,
            template=FileDataModelingTemplate.model_validate(
                dict(
                    instance_id=TemplateNodeId(
                        space=toolkit_space.space,
                        external_id=FILENAME_VARIABLE,
                    ),
                )
            ),
        )
        io = FileContentIO(toolkit_client, tmp_path)
        reader = MultiFileReader([my_text_file])

        read_chunks = list(io.read_chunks(reader, selector))
        assert len(read_chunks) == 1
        read_chunk = read_chunks[0]

        upload_content = io.json_chunk_to_data(read_chunk)
        assert len(upload_content) == 1

        try:
            with HTTPClient(toolkit_client.config) as http_client:
                io.upload_items(upload_content, http_client, selector)

            t0 = time.perf_counter()
            while True:
                uploaded_file = toolkit_client.files.retrieve(instance_id=instance_id)
                assert uploaded_file is not None
                if uploaded_file.uploaded is True:
                    break
                # The file syncer may take some time to update CogniteFile -> FileMetadata uploaded status
                if time.perf_counter() - t0 > 30:
                    raise AssertionError("Timeout waiting for file to be uploaded.")
                time.sleep(1)

            # Test download
            download_selector = FileIdentifierSelector(
                identifiers=(
                    FileInstanceID.model_validate(
                        {"instanceId": {"space": toolkit_space.space, "externalId": external_id}}
                    ),
                )
            )
            downloaded_files = [item for page in io.stream_data(download_selector) for item in page.items]
            assert len(downloaded_files) == 1
            expected_file = tmp_path / download_selector.file_directory / "untitled"
            assert expected_file.is_file()
            downloaded_content = expected_file.read_text(encoding="utf-8")
            assert downloaded_content == "This is some test content for data modeling."

        finally:
            # Clean up
            toolkit_client.data_modeling.instances.delete(nodes=instance_id)

    def test_upload_download_file_identifier(
        self, toolkit_client: ToolkitClient, tmp_path: Path, toolkit_dataset: DataSet
    ) -> None:
        external_id = f"test_file_identifier_{RUN_UNIQUE_ID}.txt"
        selector = FileIdentifierSelector(identifiers=(FileExternalID(external_id=external_id),))
        selector.dump_to_file(tmp_path)
        file_content = "This is a test file for FileIdentifierSelector."
        filepath = "file_content/test_file_identifier.txt"
        (tmp_path / filepath).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / filepath).write_text(file_content, encoding="utf-8")
        file_metadata = {
            "name": "test_file_identifier.txt",
            "externalId": external_id,
            "dataSetExternalId": toolkit_dataset.external_id,
            "source": "TestUpload",
            "mimeType": "text/plain",
            FILEPATH: "file_content/test_file_identifier.txt",
        }
        with NDJsonWriter(tmp_path, selector.kind, Uncompressed) as writer:
            writer.write_chunks([file_metadata], filestem=str(selector))

        upload_cmd = UploadCommand(silent=True)
        try:
            upload_cmd.upload(
                input_dir=tmp_path,
                client=toolkit_client,
                deploy_resources=False,
                dry_run=False,
                verbose=False,
            )

            retrieved = toolkit_client.files.retrieve(external_id=external_id)
            assert retrieved is not None

            io = FileContentIO(toolkit_client, tmp_path / "downloads")
            downloaded_files = [item for page in io.stream_data(selector) for item in page.items]
            assert len(downloaded_files) == 1
            expected_file = tmp_path / "downloads" / selector.file_directory / "test_file_identifier.txt"
            assert expected_file.is_file()
            downloaded_content = expected_file.read_text(encoding="utf-8")
            assert downloaded_content == file_content
        finally:
            toolkit_client.files.delete(external_id=external_id, ignore_unknown_ids=True)
