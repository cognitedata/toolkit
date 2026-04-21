import time
from pathlib import Path

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FILEPATH
from cognite_toolkit._cdf_tk.commands import DownloadCommand, UploadCommand
from cognite_toolkit._cdf_tk.dataio import FileMetadataContentIO
from cognite_toolkit._cdf_tk.dataio.selectors import FileMetadataFilesSelectorV2, InternalWithNameId
from tests.test_integration.constants import RUN_UNIQUE_ID


class TestFileContentV2:
    def test_upload_download_roundtrip(self, toolkit_client: ToolkitClient, tmp_path: Path) -> None:
        external_id = f"test_upload_download_file_{RUN_UNIQUE_ID}.txt"
        file_content = "This is a simple test file for the roundtrip test."

        # Setup upload directory and files
        upload_dir = tmp_path / "upload"
        upload_dir.mkdir(parents=True, exist_ok=True)

        # Create selector and dump it
        upload_selector = FileMetadataFilesSelectorV2()
        upload_selector.dump_to_file(upload_dir)

        # Create the file content to upload
        file_content_path = upload_dir / "files" / "test_file.txt"
        file_content_path.parent.mkdir(parents=True, exist_ok=True)
        file_content_path.write_text(file_content, encoding="utf-8")

        # Create CSV file with file metadata
        csv_file = upload_dir / f"{upload_selector.as_filestem()}.csv"
        csv_file.write_text(
            f"""externalId,name,mimeType,{FILEPATH}
{external_id},test_file.txt,text/plain,files/test_file.txt
""",
            encoding="utf-8",
        )

        try:
            # Upload
            upload_cmd = UploadCommand(silent=True, skip_tracking=True)
            upload_cmd.upload(
                upload_dir,
                toolkit_client,
                deploy_resources=False,
                dry_run=False,
                verbose=True,
            )

            # Wait for file to be uploaded
            self._wait_for_file_uploaded(toolkit_client, external_id)

            # Get the file id for download
            file_metadata = toolkit_client.files.retrieve(external_id=external_id)
            assert file_metadata is not None, f"File {external_id} not found after upload"
            assert file_metadata.id is not None

            # Download
            download_dir = tmp_path / "download"
            download_selector = FileMetadataFilesSelectorV2(
                download_dir_name="my_files",
                ids=(InternalWithNameId(id=file_metadata.id, name=file_metadata.name),),
            )
            download_cmd = DownloadCommand(silent=True, skip_tracking=True)
            download_cmd.download(
                selectors=[download_selector],
                io=FileMetadataContentIO(
                    toolkit_client,
                    config_directory=download_dir,
                    file_directory=download_dir / "file_content",
                ),
                output_dir=download_dir,
                verbose=True,
                file_format=".csv",
                compression="none",
                limit=100_000,
            )

            # Verify downloaded CSV contains expected metadata
            csv_files = list((download_dir / "my_files").rglob("*.csv"))
            assert len(csv_files) == 1
            csv_content = csv_files[0].read_text(encoding="utf-8")
            assert external_id in csv_content

            # Verify downloaded file content
            downloaded_files = list((download_dir / "file_content").rglob("*.txt"))
            assert len(downloaded_files) == 1
            downloaded_content = downloaded_files[0].read_text(encoding="utf-8")
            assert downloaded_content == file_content

        finally:
            # Cleanup
            toolkit_client.files.delete(external_id=external_id, ignore_unknown_ids=True)

    def _wait_for_file_uploaded(self, toolkit_client: ToolkitClient, external_id: str, timeout: float = 30.0) -> None:
        t0 = time.perf_counter()
        while True:
            uploaded_file = toolkit_client.files.retrieve(external_id=external_id)
            if uploaded_file is not None and uploaded_file.uploaded is True:
                return
            if time.perf_counter() - t0 > timeout:
                raise AssertionError(f"Timeout waiting for file {external_id} to be uploaded.")
            time.sleep(1)
