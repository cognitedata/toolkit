import json
from pathlib import Path

import httpx
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.commands import UploadCommand
from cognite_toolkit._cdf_tk.dataio.selectors import (
    FileDataModelingTemplate,
    FileDataModelingTemplateSelector,
    FileMetadataTemplate,
    FileMetadataTemplateSelector,
)
from cognite_toolkit._cdf_tk.dataio.selectors._file_content import FILENAME_VARIABLE, TemplateNodeId
from cognite_toolkit._cdf_tk.dataio.selectors._instances import SelectedView


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

    @pytest.mark.usefixtures("disable_gzip")
    def test_upload_dm_file_passes_template_properties(
        self, respx_mock: respx.MockRouter, toolkit_config: ToolkitClientConfig, tmp_path: Path
    ) -> None:
        config = toolkit_config
        file_dir = tmp_path / "test"
        file_dir.mkdir()
        (file_dir / "my_report.json").write_text('{"data": "test"}', encoding="utf-8")

        not_found = httpx.Response(400, json={"error": {"code": 400, "message": "not found", "missing": [{}]}})
        upload_url = httpx.Response(200, json={"items": [{"uploadUrl": "https://upload.test/file"}]})
        respx_mock.post(config.create_api_url("/files/uploadlink")).mock(side_effect=[not_found, upload_url])
        respx_mock.post(config.create_api_url("/models/instances")).mock(
            return_value=httpx.Response(200, json={"items": []})
        )
        respx_mock.put("https://upload.test/file").mock(return_value=httpx.Response(200))
        view_id = SelectedView(space="my_custom_space", external_id="MyFileView", version="v2")
        selector = FileDataModelingTemplateSelector(
            file_directory=file_dir,
            view_id=view_id,
            template=FileDataModelingTemplate.model_validate(
                dict(
                    instanceId=TemplateNodeId(space="my_space", externalId=FILENAME_VARIABLE),
                    name="rca_report",
                    mimeType="application/json",
                )
            ),
        )
        selector.dump_to_file(tmp_path)
        UploadCommand(silent=True).upload(
            input_dir=tmp_path, client=ToolkitClient(config), deploy_resources=False, dry_run=False, verbose=True
        )

        instances_calls = [
            c for c in respx_mock.calls if str(c.request.url) == config.create_api_url("/models/instances")
        ]
        source = json.loads(instances_calls[0].request.content)["items"][0]["sources"][0]
        assert source["source"] == {
            "type": "view",
            "space": "my_custom_space",
            "externalId": "MyFileView",
            "version": "v2",
        }
        assert source["properties"]["name"] == "rca_report"
        assert source["properties"]["mimeType"] == "application/json"
