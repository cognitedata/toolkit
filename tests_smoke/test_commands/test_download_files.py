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

_DOWNLOAD_LIMIT = 2
_FILE_METADATA_EXTERNAL_IDS = (
    "toolkit_smoke_download_filemetadata_1",
    "toolkit_smoke_download_filemetadata_2",
)
_COGNITE_FILE_EXTERNAL_IDS = (
    "toolkit_smoke_download_cognitefile_1",
    "toolkit_smoke_download_cognitefile_2",
)


class TestDownloadFiles:
    def test_download_two_files_from_smoke_dataset(
        self,
        toolkit_client: ToolkitClient,
        smoke_dataset: DataSetResponse,
        tmp_path: Path,
    ) -> None:
        if smoke_dataset.external_id is None or smoke_dataset.id is None:
            raise AssertionError("Smoke dataset is missing an id, so files cannot be downloaded from it.")
        for external_id in _FILE_METADATA_EXTERNAL_IDS:
            _delete_file_metadata(toolkit_client, external_id)
        try:
            toolkit_client.tool.filemetadata.create(
                [
                    FileMetadataRequest(
                        name=external_id,
                        external_id=external_id,
                        data_set_id=smoke_dataset.id,
                    )
                    for external_id in _FILE_METADATA_EXTERNAL_IDS
                ],
                overwrite=True,
            )
            output_dir = tmp_path / "filemetadata"
            _download_file_metadata(output_dir, smoke_dataset.external_id)
            downloaded = _count_downloaded_rows(output_dir)
            if downloaded != _DOWNLOAD_LIMIT:
                raise AssertionError(
                    f"Expected {_DOWNLOAD_LIMIT} file metadata rows from the smoke dataset, got {downloaded}.\n"
                    f"{_issue_logs(output_dir)}"
                )
        finally:
            for external_id in _FILE_METADATA_EXTERNAL_IDS:
                _delete_file_metadata(toolkit_client, external_id)

    def test_download_two_files_from_smoke_space(
        self,
        toolkit_client: ToolkitClient,
        smoke_space: SpaceResponse,
        tmp_path: Path,
    ) -> None:
        space = smoke_space.space
        for external_id in _COGNITE_FILE_EXTERNAL_IDS:
            _delete_cognite_file(toolkit_client, space, external_id)
        try:
            toolkit_client.tool.cognite_files.create(
                [
                    CogniteFileRequest(space=space, external_id=external_id, name=external_id)
                    for external_id in _COGNITE_FILE_EXTERNAL_IDS
                ],
                replace=True,
            )
            output_dir = tmp_path / "cognitefile"
            _download_cognite_files(output_dir, space)
            downloaded = _count_downloaded_rows(output_dir)
            if downloaded != _DOWNLOAD_LIMIT:
                raise AssertionError(
                    f"Expected {_DOWNLOAD_LIMIT} CogniteFile rows from the smoke space, got {downloaded}.\n"
                    f"{_issue_logs(output_dir)}"
                )
        finally:
            for external_id in _COGNITE_FILE_EXTERNAL_IDS:
                _delete_cognite_file(toolkit_client, space, external_id)


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


def _delete_file_metadata(client: ToolkitClient, external_id: str) -> None:
    try:
        client.tool.filemetadata.delete([ExternalId(external_id=external_id)], ignore_unknown_ids=True)
    except ToolkitAPIError:
        return


def _delete_cognite_file(client: ToolkitClient, space: str, external_id: str) -> None:
    node_id = NodeId(space=space, external_id=external_id)
    try:
        linked = client.tool.filemetadata.retrieve([InstanceId(instance_id=node_id)], ignore_unknown_ids=True)
    except ToolkitAPIError:
        linked = []
    try:
        client.tool.cognite_files.delete([node_id])
    except ToolkitAPIError:
        pass
    if not linked:
        return
    try:
        client.tool.filemetadata.delete([item.as_internal_id() for item in linked], ignore_unknown_ids=True)
    except ToolkitAPIError:
        return
