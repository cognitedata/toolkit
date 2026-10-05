import csv
from pathlib import Path

import click
import typer

from cognite_toolkit._cdf_tk.apps._download_app import DownloadApp
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, InstanceId, NodeId
from cognite_toolkit._cdf_tk.client.resource_classes.cognite_file import COGNITE_FILE_VIEW_ID, CogniteFileRequest
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import SpaceResponse
from cognite_toolkit._cdf_tk.client.resource_classes.dataset import DataSetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataRequest
from tests_smoke.test_commands.test_upload_files import _wait_until_cognite_file_uploaded, _wait_until_uploaded

_DOWNLOAD_LIMIT = 2
_MIME_TYPE = "text/plain"
_FILE_METADATA_CONTENT = {
    "toolkit_smoke_download_filemetadata_1": "toolkit smoke download file metadata 1\n",
    "toolkit_smoke_download_filemetadata_2": "toolkit smoke download file metadata 2\n",
}
_COGNITE_FILE_CONTENT = {
    "toolkit_smoke_download_cognitefile_1": "toolkit smoke download cognite file 1\n",
    "toolkit_smoke_download_cognitefile_2": "toolkit smoke download cognite file 2\n",
}


class TestDownloadFiles:
    def test_download_two_files_from_smoke_dataset(
        self,
        toolkit_client: ToolkitClient,
        smoke_dataset: DataSetResponse,
        tmp_path: Path,
    ) -> None:
        if smoke_dataset.external_id is None or smoke_dataset.id is None:
            raise AssertionError("Smoke dataset is missing an id, so files cannot be downloaded from it.")
        for external_id, content in _FILE_METADATA_CONTENT.items():
            _ensure_file_metadata_content(toolkit_client, smoke_dataset.id, external_id, content)
        output_dir = tmp_path / "filemetadata"
        _download_file_metadata(output_dir, smoke_dataset.external_id)
        _assert_downloaded_row_count(output_dir, "file metadata rows from the smoke dataset")

    def test_download_two_files_from_smoke_space(
        self,
        toolkit_client: ToolkitClient,
        smoke_space: SpaceResponse,
        tmp_path: Path,
    ) -> None:
        space = smoke_space.space
        for external_id, content in _COGNITE_FILE_CONTENT.items():
            _ensure_cognite_file_content(toolkit_client, space, external_id, content)
        output_dir = tmp_path / "cognitefile"
        _download_cognite_files(output_dir, space)
        _assert_downloaded_row_count(output_dir, "CogniteFile rows from the smoke space")


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


def _ensure_cognite_file_content(client: ToolkitClient, space: str, external_id: str, content: str) -> None:
    node_id = NodeId(space=space, external_id=external_id)
    try:
        nodes = client.tool.cognite_files.retrieve([node_id])
    except ToolkitAPIError:
        nodes = []
    if nodes and nodes[0].is_uploaded:
        return
    if not nodes:
        client.tool.cognite_files.create(
            [CogniteFileRequest(space=space, external_id=external_id, name=external_id, mime_type=_MIME_TYPE)]
        )
    instance_id = InstanceId(instance_id=node_id)
    upload_url = _upload_url(client, instance_id, f"{space}:{external_id}")
    client.tool.filemetadata.upload_file(content, upload_url, _MIME_TYPE)
    _wait_until_uploaded(client, instance_id)
    _wait_until_cognite_file_uploaded(client, node_id)


def _upload_url(client: ToolkitClient, identifier: ExternalId | InstanceId, label: str) -> str:
    linked = client.tool.filemetadata.get_upload_url([identifier])
    if not linked or not linked[0].upload_url:
        raise AssertionError(f"CDF did not return an upload URL for {label}.")
    return linked[0].upload_url


def _download_file_metadata(output_dir: Path, data_set_external_id: str) -> None:
    try:
        DownloadApp().download_files_cmd(
            typer.Context(click.Command("download_files")),
            data_sets=[data_set_external_id],
            output_dir=output_dir,
            limit=_DOWNLOAD_LIMIT,
            verbose=True,
        )
    except Exception as error:
        raise AssertionError(f"DownloadApp.download_files_cmd failed: {error}\n{_issue_logs(output_dir)}") from error


def _download_cognite_files(output_dir: Path, space: str) -> None:
    view = COGNITE_FILE_VIEW_ID
    view_id = f"{view.external_id}/{view.version}"
    try:
        DownloadApp().download_instances_cmd(
            typer.Context(click.Command("download_instances")),
            instance_spaces=[space],
            schema_space=view.space,
            view_external_ids=[view_id],
            output_dir=output_dir,
            limit=_DOWNLOAD_LIMIT,
            verbose=True,
        )
    except Exception as error:
        raise AssertionError(
            f"DownloadApp.download_instances_cmd failed: {error}\n{_issue_logs(output_dir)}"
        ) from error


def _assert_downloaded_row_count(output_dir: Path, label: str) -> None:
    downloaded = _count_downloaded_rows(output_dir)
    if downloaded != _DOWNLOAD_LIMIT:
        raise AssertionError(f"Expected {_DOWNLOAD_LIMIT} {label}, got {downloaded}.\n{_issue_logs(output_dir)}")


def _count_downloaded_rows(output_dir: Path) -> int:
    csv_files = _data_files(output_dir, ".csv")
    if csv_files:
        return sum(_csv_data_rows(path) for path in csv_files)
    return sum(_ndjson_data_rows(path) for path in _data_files(output_dir, ".ndjson"))


def _data_files(output_dir: Path, suffix: str) -> list[Path]:
    if not output_dir.exists():
        return []
    return [
        path
        for path in output_dir.rglob(f"*{suffix}")
        if path.is_file() and "Issues" not in path.name and path.suffix == suffix
    ]


def _csv_data_rows(path: Path) -> int:
    with path.open(encoding="utf-8", newline="") as handle:
        return sum(1 for _ in csv.DictReader(handle))


def _ndjson_data_rows(path: Path) -> int:
    return sum(1 for line in path.read_text(encoding="utf-8").splitlines() if line.strip())


def _issue_logs(directory: Path) -> str:
    if not directory.exists():
        return "No download issue log was written."
    logs = sorted(path for path in directory.rglob("*DownloadIssues*") if path.is_file())
    if not logs:
        return "No download issue log was written."
    sections = [f"{path.name}:\n{path.read_text(encoding='utf-8')}" for path in logs]
    return "\n".join(sections)
