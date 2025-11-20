from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.commands import UploadCommand
from cognite_toolkit._cdf_tk.storageio import FileContentIO
from cognite_toolkit._cdf_tk.storageio.selectors import FileMetadataTemplate, FileMetadataTemplateSelector
from cognite_toolkit._cdf_tk.storageio.selectors._file_content import FILENAME_VARIABLE


@pytest.fixture
def file_folder(tmp_path: Path) -> Path:
    # Create some dummy file content
    file_folder = tmp_path / "files"
    file_folder.mkdir(parents=True, exist_ok=True)
    for i in range(5):
        file_path = file_folder / f"file_{i}.txt"
        file_path.write_text(f"This is the content of file {i}.", encoding="utf-8")
    return file_folder


class TestFileContent:
    def test_upload_filemetadata(self, toolkit_config: ToolkitClientConfig, file_folder: Path) -> None:
        config = toolkit_config
        client = ToolkitClient(config)
        selector = FileMetadataTemplateSelector(
            file_directory=file_folder,
            template=FileMetadataTemplate.model_validate(
                dict(
                    name=FILENAME_VARIABLE,
                    external_id=f"my_file_{FILENAME_VARIABLE}",
                    source="Uploaded via Toolkit",
                )
            ),
        )
        selector.dump_to_file(file_folder.parent)
        cmd = UploadCommand(silent=True)
        cmd.upload(
            input_dir=file_folder.parent,
            client=client,
            deploy_resources=False,
            dry_run=False,
            verbose=True,
            kind=FileContentIO.KIND,
        )
