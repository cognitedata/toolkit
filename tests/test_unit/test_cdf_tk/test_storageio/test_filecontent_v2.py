from pathlib import Path
from unittest.mock import MagicMock

from cognite_toolkit._cdf_tk.client.http_client import HTTPClient
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
        (file_directory / "my_file.txt").write_text("This is a test file.")
        (file_directory / "my_file.json").write_text('{"key": "value"}')

        selector = FileMetadataTemplateSelector(
            template=FileMetadataTemplate.model_validate(
                dict(
                    name=FILENAME_VARIABLE,
                    external_id=f"my_id_{FILENAME_VARIABLE}",
                    directory="/my_directory",
                )
            ),
            file_directory=file_directory,
        )
        selector.dump_to_file(tmp_path)

        with monkeypatch_toolkit_client() as client:
            io = FileMetadataContentIO(client, overwrite=True)
            files = selector.find_data_files(tmp_path, tmp_path / selector.as_filename())
            assert len(files) == 2
            reader = MultiFileReader(files)

            chunks = io.read_chunks(reader, selector)
            requests = (io.json_chunk_to_data(page) for page in chunks)
            result = [io.upload_items(page, MagicMock(spec=HTTPClient), selector) for page in requests]

            assert len(result) == len(files)
