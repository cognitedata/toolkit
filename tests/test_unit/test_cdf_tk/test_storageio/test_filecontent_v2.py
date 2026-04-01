from pathlib import Path
from unittest.mock import MagicMock

from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataResponse
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.storageio._file_contentv2 import (
    FILENAME_VARIABLE,
    FileMetadataContentIO,
    FileMetadataTemplate,
    FileMetadataTemplateSelector,
)
from cognite_toolkit._cdf_tk.utils.fileio import MultiFileReader


class TestFileMetadataContentIO:
    def test_upload_template(self, tmp_path: Path):
        file_directory = tmp_path / "target"
        file_directory.mkdir()
        text_file = file_directory / "my_file.txt"
        json_file = file_directory / "my_file.json"
        text_file.write_text("This is a test file.")
        json_file.write_text('{"key": "value"}')

        selector = FileMetadataTemplateSelector(
            template=FileMetadataTemplate.model_validate(
                dict(
                    name=FILENAME_VARIABLE,
                    external_id=f"my_id_{FILENAME_VARIABLE}",
                    directory="/my_directory",
                )
            ),
            file_directory=file_directory,
            guess_mime_type=True,
        )
        selector.dump_to_file(tmp_path)

        with monkeypatch_toolkit_client() as client:
            client.tool.filemetadata.create.return_value = [
                FileMetadataResponse(
                    name="dummy",
                    created_time=1,
                    last_updated_time=1,
                    uploaded=False,
                    upload_url="https://some.url",
                    id=37,
                )
            ]
            client.tool.filemetadata.upload_file.return_value = SuccessResponse(status_code=200, body="", content=b"")

            io = FileMetadataContentIO(client, overwrite=True)
            files = selector.find_data_files(tmp_path, tmp_path / selector.as_filename())

            chunks = io.read_chunks(MultiFileReader(files), selector)
            requests = (io.json_chunk_to_data(page) for page in chunks)
            result_pages = [io.upload_items(page, MagicMock(spec=HTTPClient), selector) for page in requests]
            assert len(result_pages) == 1
            results = sorted(result_pages[0], key=lambda x: x.ids[0])

            assert results == [
                ItemsSuccessResponse(ids=[json_file.as_posix()], status_code=200, body="", content=b""),
                ItemsSuccessResponse(ids=[text_file.as_posix()], status_code=200, body="", content=b""),
            ]
