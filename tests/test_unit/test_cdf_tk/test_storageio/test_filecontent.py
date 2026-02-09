import json
from pathlib import Path

import httpx
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.commands import UploadCommand
from cognite_toolkit._cdf_tk.storageio.selectors import FileMetadataTemplate, FileMetadataTemplateSelector
from cognite_toolkit._cdf_tk.storageio.selectors._file_content import FILENAME_VARIABLE


@pytest.fixture
def file_folder(tmp_path: Path) -> Path:
    # Create some dummy file content
    file_folder = tmp_path / "files"
    file_folder.mkdir(parents=True, exist_ok=True)
    for i in range(5):
        file_path = file_folder / f"file_{i}.csv"
        file_path.write_text(f"This is the content of file {i}.", encoding="utf-8")
    return file_folder


class TestFileContent:
    @pytest.mark.usefixtures("disable_gzip")
    def test_upload_filemetadata(
        self, respx_mock: respx.MockRouter, toolkit_config: ToolkitClientConfig, file_folder: Path
    ) -> None:
        config = toolkit_config
        client = ToolkitClient(config)
        file_upload_url = "https://upload.url/for/testing/{externalId}"

        def create_callback(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content)
            assert "name" in payload
            assert "externalId" in payload
            payload["uploadUrl"] = file_upload_url.format(externalId=payload["externalId"])
            return httpx.Response(status_code=200, json=payload, headers={})

        respx_mock.post(config.create_api_url("/files")).mock(side_effect=create_callback)
        upload_endpoints: list[str] = []
        for filepath in file_folder.iterdir():
            if filepath.is_file():
                upload_endpoint = file_upload_url.format(externalId=f"my_file_{filepath.name}")
                respx_mock.put(upload_endpoint).mock(return_value=httpx.Response(status_code=200))
                upload_endpoints.append(upload_endpoint)

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
        )

        assert respx_mock.calls.call_count == 10  # 5 for metadata, 5 for content
        called_endpoints = {str(call.request.url) for call in respx_mock.calls}
        assert set(upload_endpoints).issubset(called_endpoints)
