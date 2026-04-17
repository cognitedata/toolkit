from pathlib import Path
from unittest.mock import MagicMock

from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsResultMessage,
    ItemsSuccessResponse,
    SuccessResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FILEPATH, FileMetadataResponse
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.dataio._file_contentv2 import (
    FileMetadataContentIO,
)
from cognite_toolkit._cdf_tk.dataio.selectors import (
    FILENAME_VARIABLE,
    FileMetadataContentSelectorV2,
    FileMetadataFilesSelectorV2,
    FileMetadataTemplateSelectorV2,
    FileMetadataTemplateV2,
)
from cognite_toolkit._cdf_tk.utils.fileio import MultiFileReader


class TestFileMetadataContentIO:
    def test_upload_using_template(self, tmp_path: Path) -> None:
        file_directory = tmp_path / "target"
        file_directory.mkdir()
        text_file = file_directory / "my_file.txt"
        json_file = file_directory / "my_file.json"
        text_file.write_text("This is a test file.")
        json_file.write_text('{"key": "value"}')

        selector = FileMetadataTemplateSelectorV2(
            template=FileMetadataTemplateV2.model_validate(
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

        results = self._upload_files(selector, tmp_path)

        assert results == [
            ItemsSuccessResponse(ids=[json_file.as_posix()], status_code=200, body="", content=b""),
            ItemsSuccessResponse(ids=[text_file.as_posix()], status_code=200, body="", content=b""),
        ]

    def test_upload_using_identifier(self, tmp_path: Path) -> None:
        file_directory = tmp_path / "target"
        file_directory.mkdir()
        text_file = file_directory / "my_file.txt"
        json_file = file_directory / "my_file.json"
        text_file.write_text("This is a test file.")
        json_file.write_text('{"key": "value"}')

        selector = FileMetadataFilesSelectorV2()
        selector.dump_to_file(tmp_path)
        csv_file = f"""externalId,name,{FILEPATH}\n
my_json_file,my_json_file.json,{json_file.relative_to(tmp_path)}\n
my_text_file,my_text_file.txt,{text_file.relative_to(tmp_path)}\n
"""
        (tmp_path / f"{selector.as_filestem()}.csv").write_text(csv_file)

        results = self._upload_files(selector, tmp_path)

        assert results == [
            ItemsSuccessResponse(ids=["row 1"], status_code=200, body="", content=b""),
            ItemsSuccessResponse(ids=["row 2"], status_code=200, body="", content=b""),
        ]

    def _upload_files(self, selector: FileMetadataContentSelectorV2, tmp_path: Path) -> list[ItemsResultMessage]:
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

            io = FileMetadataContentIO(client, overwrite=True, config_directory=tmp_path)
            files = selector.find_data_files(tmp_path, tmp_path / selector.as_filename())

            chunks = io.read_chunks(MultiFileReader(files), selector)
            requests = (io.json_chunk_to_data(page) for page in chunks)
            result_pages = [io.upload_items(page, MagicMock(spec=HTTPClient), selector) for page in requests]
            assert len(result_pages) == 1
            results = sorted(result_pages[0], key=lambda x: x.ids[0])
        return results
