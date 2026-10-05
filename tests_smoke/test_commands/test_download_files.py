from pathlib import Path

import click
import pytest
import typer

from cognite_toolkit._cdf_tk.apps._download_app import AssetCentricFormats, DownloadApp
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, InstanceId
from cognite_toolkit._cdf_tk.client.resource_classes.dataset import DataSetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.documents import DocumentResponse, DocumentSourceFile
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataRequest
from cognite_toolkit._cdf_tk.utils.interactive_select import DocumentSelectStatus, SelectedDocuments
from tests.test_unit.utils import MockQuestionary
from tests_smoke.test_commands.test_upload_files import _wait_until_uploaded

_DOWNLOAD_LIMIT = 2
_MIME_TYPE = "text/plain"
_FILE_METADATA_DIR = "asset-centric-files-with-content"
_FILE_METADATA_CONTENT = {
    "toolkit_smoke_download_filemetadata_1": "toolkit smoke download file metadata 1\n",
    "toolkit_smoke_download_filemetadata_2": "toolkit smoke download file metadata 2\n",
}


class TestDownloadFiles:
    def test_download_two_files_from_smoke_dataset(
        self,
        toolkit_client: ToolkitClient,
        smoke_dataset: DataSetResponse,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        if smoke_dataset.external_id is None:
            raise AssertionError("Smoke dataset is missing an external_id, so files cannot be downloaded from it.")
        for external_id, content in _FILE_METADATA_CONTENT.items():
            _ensure_file_metadata_content(toolkit_client, smoke_dataset.id, external_id, content)
        output_dir = tmp_path / "filemetadata"
        _download_file_content(
            monkeypatch,
            output_dir,
            data_set_external_id=smoke_dataset.external_id,
            documents=_file_metadata_documents(toolkit_client, list(_FILE_METADATA_CONTENT)),
            cognite_file=False,
        )
        _assert_downloaded_content(output_dir, _FILE_METADATA_DIR, _FILE_METADATA_CONTENT)


def _ensure_file_metadata_content(client: ToolkitClient, data_set_id: int, external_id: str, content: str) -> None:
    identifier = ExternalId(external_id=external_id)
    found = client.tool.filemetadata.retrieve([identifier], ignore_unknown_ids=True)
    if found and found[0].uploaded:
        return
    if found:
        upload_url = _upload_url(client, identifier, external_id)
    else:
        created = client.tool.filemetadata.create(
            [
                FileMetadataRequest(
                    name=external_id,
                    external_id=external_id,
                    data_set_id=data_set_id,
                    mime_type=_MIME_TYPE,
                )
            ]
        )[0]
        if not created.upload_url:
            raise AssertionError(f"CDF did not return an upload URL for file metadata {external_id}.")
        upload_url = created.upload_url
    client.tool.filemetadata.upload_file(content, upload_url, _MIME_TYPE)
    _wait_until_uploaded(client, identifier)


def _upload_url(client: ToolkitClient, identifier: ExternalId | InstanceId, label: str) -> str:
    linked = client.tool.filemetadata.get_upload_url([identifier])
    if not linked or not linked[0].upload_url:
        raise AssertionError(f"CDF did not return an upload URL for {label}.")
    return linked[0].upload_url


def _file_metadata_documents(client: ToolkitClient, external_ids: list[str]) -> list[DocumentResponse]:
    found = client.tool.filemetadata.retrieve(
        [ExternalId(external_id=external_id) for external_id in external_ids],
        ignore_unknown_ids=True,
    )
    found_by_external_id = {item.external_id: item for item in found if item.external_id is not None}
    missing = [external_id for external_id in external_ids if external_id not in found_by_external_id]
    if missing:
        raise AssertionError(f"File metadata was not found for {', '.join(missing)}.")
    return [
        DocumentResponse(
            id=found_by_external_id[external_id].id,
            external_id=external_id,
            created_time=found_by_external_id[external_id].created_time,
            source_file=DocumentSourceFile(name=found_by_external_id[external_id].name),
        )
        for external_id in external_ids
    ]


def _download_file_content(
    monkeypatch: pytest.MonkeyPatch,
    output_dir: Path,
    data_set_external_id: str,
    documents: list[DocumentResponse],
    cognite_file: bool,
) -> None:
    selected = SelectedDocuments(documents=documents, selection=DocumentSelectStatus(is_cognite_file=cognite_file))
    monkeypatch.setattr(
        "cognite_toolkit._cdf_tk.apps._download_app.DocumentsInteractiveSelect.select_documents",
        lambda self: selected,
    )
    try:
        with MockQuestionary(
            DownloadApp.__module__,
            monkeypatch,
            [AssetCentricFormats.csv, str(output_dir)],
        ):
            DownloadApp().download_files_cmd(
                typer.Context(click.Command("download_files")),
                data_sets=[data_set_external_id],
                include_file_contents=True,
                output_dir=output_dir,
                limit=_DOWNLOAD_LIMIT,
                verbose=True,
            )
    except Exception as error:
        raise AssertionError(f"DownloadApp.download_files_cmd failed: {error}\n{_issue_logs(output_dir)}") from error


def _assert_downloaded_content(output_dir: Path, download_dir_name: str, expected: dict[str, str]) -> None:
    content_dir = output_dir / download_dir_name / "files"
    downloaded = [path for path in content_dir.rglob("*") if path.is_file()] if content_dir.is_dir() else []
    contents = sorted(path.read_text(encoding="utf-8") for path in downloaded)
    expected_contents = sorted(expected.values())
    if contents != expected_contents:
        raise AssertionError(
            "Downloaded file content did not match the uploaded files.\n"
            f"Expected {expected_contents!r}, got {contents!r}.\n"
            f"{_issue_logs(output_dir)}"
        )


def _issue_logs(directory: Path) -> str:
    if not directory.exists():
        return "No download issue log was written."
    logs = sorted(path for path in directory.rglob("*DownloadIssues*") if path.is_file())
    if not logs:
        return "No download issue log was written."
    sections = [f"{path.name}:\n{path.read_text(encoding='utf-8')}" for path in logs]
    return "\n".join(sections)
