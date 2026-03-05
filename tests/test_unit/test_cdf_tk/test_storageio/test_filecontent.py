import json
from pathlib import Path

import httpx
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient
from cognite_toolkit._cdf_tk.commands import UploadCommand
from cognite_toolkit._cdf_tk.storageio import FileContentIO
from cognite_toolkit._cdf_tk.storageio.selectors import (
    FileDataModelingTemplate,
    FileDataModelingTemplateSelector,
    FileMetadataTemplate,
    FileMetadataTemplateSelector,
)
from cognite_toolkit._cdf_tk.storageio.selectors._file_content import FILENAME_VARIABLE, TemplateNodeId
from cognite_toolkit._cdf_tk.storageio.selectors._instances import SelectedView
from cognite_toolkit._cdf_tk.utils.fileio import MultiFileReader


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
        client = ToolkitClient(config)
        file_upload_url = "https://upload.url/for/testing/dm_file"
        file_dir = tmp_path / "rca"
        file_dir.mkdir()
        test_file = file_dir / "my_report.json"
        test_file.write_text('{"data": "test"}', encoding="utf-8")

        created_nodes: list[dict] = []

        def uploadlink_callback(request: httpx.Request) -> httpx.Response:
            # First call returns 404 (node not found), second call succeeds
            if len(created_nodes) == 0:
                return httpx.Response(
                    status_code=400,
                    json={"error": {"code": 400, "message": "not found", "missing": [{"space": "my_space"}]}},
                )
            return httpx.Response(status_code=200, json={"items": [{"uploadUrl": file_upload_url}]})

        def instances_callback(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content)
            created_nodes.append(payload)
            return httpx.Response(status_code=200, json={"items": []})

        respx_mock.post(config.create_api_url("/files/uploadlink")).mock(side_effect=uploadlink_callback)
        respx_mock.post(config.create_api_url("/models/instances")).mock(side_effect=instances_callback)
        respx_mock.put(file_upload_url).mock(return_value=httpx.Response(status_code=200))

        custom_view = SelectedView(space="my_custom_space", external_id="MyFileView", version="v2")
        selector = FileDataModelingTemplateSelector(
            file_directory=file_dir,
            view_id=custom_view,
            template=FileDataModelingTemplate.model_validate(
                dict(
                    instanceId=TemplateNodeId(space="my_space", externalId=FILENAME_VARIABLE),
                    name="rca_report",
                    mimeType="application/json",
                )
            ),
        )

        io = FileContentIO(client, tmp_path)
        reader = MultiFileReader([test_file])
        read_chunks = list(io.read_chunks(reader, selector))
        assert len(read_chunks) == 1
        upload_content = io.json_chunk_to_data(read_chunks[0])
        assert len(upload_content) == 1

        with HTTPClient(config) as http_client:
            io.upload_items(upload_content, http_client, selector)

        assert len(created_nodes) == 1
        node_payload = created_nodes[0]["items"][0]
        assert node_payload["space"] == "my_space"
        assert node_payload["externalId"] == "my_report.json"

        sources = node_payload["sources"]
        assert len(sources) == 1
        source = sources[0]["source"]
        assert source["space"] == "my_custom_space"
        assert source["externalId"] == "MyFileView"
        assert source["version"] == "v2"

        properties = sources[0]["properties"]
        assert properties["name"] == "rca_report"
        assert properties["mimeType"] == "application/json"
