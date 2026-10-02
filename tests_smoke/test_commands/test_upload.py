import time
import traceback
from pathlib import Path

import pytest
from typer.testing import CliRunner

from cognite_toolkit._cdf import _app
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.api.filemetadata import FileMetadataAPI
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, InstanceId, NodeId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import SpaceResponse
from cognite_toolkit._cdf_tk.client.resource_classes.dataset import DataSetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import (
    FILEPATH,
    FileMetadataRequest,
    FileMetadataResponse,
)
from cognite_toolkit._cdf_tk.dataio.selectors import CogniteFileFilesSelectorV2, FileMetadataFilesSelectorV2

# Production code starts multipart upload at 100 MiB (2 x 50 MiB). Each part except the last must be
# larger than 5 MiB, so this payload is 12 MiB and the ideal part size is patched down to 6 MiB.
_PART_BYTES = 6 * 1024 * 1024
_PAYLOAD_BYTES = 2 * _PART_BYTES
_UPLOAD_TIMEOUT_SECONDS = 90.0

_FILE_METADATA_EXTERNAL_ID = "toolkit_smoke_multipart_file_metadata"
_COGNITE_FILE_EXTERNAL_ID = "toolkit_smoke_multipart_cognite_file"


@pytest.fixture
def multipart_upload(monkeypatch: pytest.MonkeyPatch) -> dict[str, list[int]]:
    """Patch the multipart threshold and record which multipart endpoint each upload uses."""
    monkeypatch.setattr("cognite_toolkit._cdf_tk.dataio._file_contentv2.IDEAL_FILE_SIZE", _PART_BYTES)
    calls: dict[str, list[int]] = {"init": [], "link": []}
    original_init = FileMetadataAPI.upload_multi_parts
    original_link = FileMetadataAPI.get_multipart_upload_urls

    def wrapped_init(
        self: FileMetadataAPI, item: FileMetadataRequest, overwrite: bool, parts: int
    ) -> FileMetadataResponse:
        calls["init"].append(parts)
        return original_init(self, item, overwrite, parts)

    def wrapped_link(self: FileMetadataAPI, item: ExternalId | InstanceId, parts: int) -> FileMetadataResponse:
        calls["link"].append(parts)
        return original_link(self, item, parts)

    monkeypatch.setattr(FileMetadataAPI, "upload_multi_parts", wrapped_init)
    monkeypatch.setattr(FileMetadataAPI, "get_multipart_upload_urls", wrapped_link)
    return calls


class TestUploadMultipart:
    def test_file_metadata_content_uses_multipart(
        self,
        toolkit_client: ToolkitClient,
        smoke_dataset: DataSetResponse,
        tmp_path: Path,
        multipart_upload: dict[str, list[int]],
    ) -> None:
        upload_dir = tmp_path / "filemetadata"
        _prepare_file_metadata_upload(upload_dir, smoke_dataset.external_id)
        _delete_file_metadata(toolkit_client, _FILE_METADATA_EXTERNAL_ID)
        try:
            _run_upload_dir(upload_dir, toolkit_client.config.project, multipart_upload)
            _require_multipart(multipart_upload, endpoint="init", label="file metadata")
            _wait_until_uploaded(toolkit_client, ExternalId(external_id=_FILE_METADATA_EXTERNAL_ID))
        finally:
            _delete_file_metadata(toolkit_client, _FILE_METADATA_EXTERNAL_ID)

    def test_cognite_file_content_uses_multipart(
        self,
        toolkit_client: ToolkitClient,
        smoke_space: SpaceResponse,
        tmp_path: Path,
        multipart_upload: dict[str, list[int]],
    ) -> None:
        upload_dir = tmp_path / "cognitefile"
        space = smoke_space.space
        _prepare_cognite_file_upload(upload_dir, space)
        _delete_cognite_file(toolkit_client, space, _COGNITE_FILE_EXTERNAL_ID)
        try:
            _run_upload_dir(upload_dir, toolkit_client.config.project, multipart_upload)
            _require_multipart(multipart_upload, endpoint="link", label="CogniteFile")
            node_id = NodeId(space=space, external_id=_COGNITE_FILE_EXTERNAL_ID)
            _wait_until_uploaded(toolkit_client, InstanceId(instance_id=node_id))
            _wait_until_cognite_file_uploaded(toolkit_client, node_id)
        finally:
            _delete_cognite_file(toolkit_client, space, _COGNITE_FILE_EXTERNAL_ID)


def _prepare_file_metadata_upload(upload_dir: Path, data_set_external_id: str | None) -> None:
    upload_dir.mkdir(parents=True)
    if data_set_external_id is None:
        raise AssertionError("Smoke dataset is missing an external ID, so the file cannot be uploaded.")
    selector = FileMetadataFilesSelectorV2()
    selector.dump_to_file(upload_dir)
    payload = upload_dir / "payload.bin"
    _write_payload(payload)
    csv_path = upload_dir / f"{selector.as_filestem()}.csv"
    csv_path.write_text(
        "\n".join(
            [
                f"externalId,name,mimeType,dataSetExternalId,{FILEPATH}",
                f"{_FILE_METADATA_EXTERNAL_ID},smoke-multipart.bin,application/octet-stream,{data_set_external_id},{payload.name}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def _prepare_cognite_file_upload(upload_dir: Path, space: str) -> None:
    upload_dir.mkdir(parents=True)
    selector = CogniteFileFilesSelectorV2()
    selector.dump_to_file(upload_dir)
    payload = upload_dir / "payload.bin"
    _write_payload(payload)
    csv_path = upload_dir / f"{selector.as_filestem()}.csv"
    csv_path.write_text(
        "\n".join(
            [
                f"space,externalId,name,mimeType,{FILEPATH}",
                f"{space},{_COGNITE_FILE_EXTERNAL_ID},smoke-multipart.bin,application/octet-stream,{payload.name}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def _write_payload(path: Path) -> None:
    chunk = b"toolkit-smoke-multipart\n"
    path.write_bytes(chunk * ((_PAYLOAD_BYTES // len(chunk)) + 1))


def _run_upload_dir(upload_dir: Path, project: str, calls: dict[str, list[int]]) -> None:
    result = CliRunner().invoke(
        _app,
        [
            "data",
            "upload",
            "dir",
            str(upload_dir),
            "--cdf-project",
            project,
            "--skip-verify-cdf-project",
            "--overwrite",
            "--verbose",
        ],
    )
    if result.exit_code == 0:
        return
    exception = ""
    if result.exc_info is not None:
        exception = "".join(traceback.format_exception(*result.exc_info))
    raise AssertionError(
        "cdf data upload dir failed with exit code "
        f"{result.exit_code}. Multipart init calls: {calls['init']}. "
        f"Multipart link calls: {calls['link']}.\n{result.output}\n{_issue_logs(upload_dir)}\n{exception}"
    )


_MULTIPART_ENDPOINTS = {
    "init": "POST /files/initmultipartupload",
    "link": "POST /files/multiuploadlink",
}


def _require_multipart(calls: dict[str, list[int]], endpoint: str, label: str) -> None:
    parts = calls[endpoint]
    other = "link" if endpoint == "init" else "init"
    expected = _MULTIPART_ENDPOINTS[endpoint]
    if not parts or parts[0] < 2:
        raise AssertionError(
            f"The {label} upload did not use multipart upload. "
            f"Expected {expected} with at least 2 parts, got {parts or 'no call'}."
        )
    if calls[other]:
        raise AssertionError(
            f"The {label} upload called {_MULTIPART_ENDPOINTS[other]} ({calls[other]} parts). "
            f"Only {expected} should have been called."
        )


def _wait_until_uploaded(client: ToolkitClient, identifier: ExternalId | InstanceId) -> None:
    found = client.tool.filemetadata.retrieve([identifier], ignore_unknown_ids=True)
    if not found:
        raise AssertionError(f"CDF did not return file metadata for {identifier} after cdf data upload dir.")
    pending, _elapsed = client.tool.filemetadata.await_file_uploaded(
        [found[0].as_internal_id()], timeout_seconds=_UPLOAD_TIMEOUT_SECONDS
    )
    if pending:
        raise AssertionError(
            f"File {identifier} was not marked as uploaded within {_UPLOAD_TIMEOUT_SECONDS:.0f} seconds."
        )


def _wait_until_cognite_file_uploaded(client: ToolkitClient, node_id: NodeId) -> None:
    deadline = time.monotonic() + _UPLOAD_TIMEOUT_SECONDS
    last_uploaded: bool | None = None
    while time.monotonic() < deadline:
        try:
            nodes = client.tool.cognite_files.retrieve([node_id])
        except ToolkitAPIError:
            nodes = []
        if nodes and nodes[0].is_uploaded:
            return
        last_uploaded = nodes[0].is_uploaded if nodes else None
        time.sleep(2)
    raise AssertionError(
        f"CogniteFile {node_id.space}:{node_id.external_id} was not marked as uploaded "
        f"within {_UPLOAD_TIMEOUT_SECONDS:.0f} seconds (last isUploaded={last_uploaded})."
    )


def _issue_logs(upload_dir: Path) -> str:
    logs = sorted(upload_dir.glob("*UploadIssues*"))
    if not logs:
        return "No upload issue log was written."
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
