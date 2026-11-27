from collections.abc import Iterable
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from _pytest.monkeypatch import MonkeyPatch

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.identifiers import NodeId
from cognite_toolkit._cdf_tk.client.resource_classes.cognite_file import CogniteFileRequest, CogniteFileResponse
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataRequest, FileMetadataResponse
from cognite_toolkit._cdf_tk.resource_ios import CogniteFileIO, FileMetadataIO
from tests.test_unit.approval_client.client import LookUpAPIMock


def file_metadata_config_cases() -> Iterable[tuple]:
    yield pytest.param(
        """externalId: sharepointABC
name: A file.txt
dataSetExternalId: ds_files
source: sharepointABC
$FILEPATH: some_file.txt
""",
        ["1.A file.txt"],
        [
            FileMetadataRequest(
                external_id="sharepointABC",
                source="sharepointABC",
                name="A file.txt",
                data_set_id=LookUpAPIMock.create_id("ds_files"),
                metadata={FileMetadataIO._MetadataKey.FILECONTENT_HASH: "hash"},
            )
        ],
        id="Single file as mapping",
    )
    yield pytest.param(
        """- externalId: sharepointABC
  name: A file.txt
  dataSetExternalId: ds_files
  source: sharepointABC
  $FILEPATH: some_file.txt
- externalId: sharepointABC2
  name: Another file.txt
  dataSetExternalId: ds_files
  source: sharepointABC
  $FILEPATH: some_other_file.txt
""",
        ["1.A file.txt", "1.Another file.txt"],
        [
            FileMetadataRequest(
                external_id="sharepointABC",
                source="sharepointABC",
                name="A file.txt",
                data_set_id=LookUpAPIMock.create_id("ds_files"),
                metadata={FileMetadataIO._MetadataKey.FILECONTENT_HASH: "hash"},
            ),
            FileMetadataRequest(
                external_id="sharepointABC2",
                source="sharepointABC",
                name="Another file.txt",
                data_set_id=LookUpAPIMock.create_id("ds_files"),
                metadata={FileMetadataIO._MetadataKey.FILECONTENT_HASH: "hash"},
            ),
        ],
        id="Multiple files as array",
    )


class TestLoadResources:
    @pytest.mark.parametrize("yaml_content, files, expected", list(file_metadata_config_cases()))
    def test_load_resources(
        self,
        yaml_content: str,
        files: list[str],
        expected: list[FileMetadataRequest],
        toolkit_client_with_lookup: ToolkitClient,
        monkeypatch: MonkeyPatch,
    ) -> None:
        fileio = FileMetadataIO(toolkit_client_with_lookup)
        filepath = MagicMock(spec=Path)
        filepath.read_text.return_value = yaml_content
        filepath.parent.glob.return_value = [Path(f) for f in files]

        with patch(f"{FileMetadataIO.__module__}.calculate_hash", return_value="hash"):
            resources = fileio.load_resource_files([filepath], is_dry_run=False)

        assert [resource.dump() for resource in resources] == [item.dump() for item in expected]

    @staticmethod
    def always_existing_path(*_) -> Path:
        path = MagicMock(spec=Path)
        path.exists.return_value = True
        return path

    def test_dump_file_metadata_without_dataset(
        self, monkeypatch: MonkeyPatch, toolkit_client_cheap: ToolkitClient
    ) -> None:
        metadata = FileMetadataResponse(
            external_id="my_file",
            name="my_file.txt",
            mime_type="text/plain",
            id=123,
            created_time=0,
            last_updated_time=0,
            uploaded=True,
        )
        loader = FileMetadataIO(toolkit_client_cheap)

        dumped = loader.dump_resource(metadata)

        assert dumped == {
            "externalId": "my_file",
            "name": "my_file.txt",
            "mimeType": "text/plain",
        }


class TestFileMetadataIOUpdate:
    @pytest.mark.parametrize(
        "cdf_hash, local_hash, expect_upload",
        [
            pytest.param("old", "new", True, id="hash_changed_triggers_reupload"),
            pytest.param("same", "same", False, id="hash_unchanged_skips_reupload"),
        ],
    )
    def test_reupload_based_on_hash(self, cdf_hash: str, local_hash: str, expect_upload: bool) -> None:
        mock_client = MagicMock()
        fileio = FileMetadataIO(mock_client, support_upload=True)

        def _response(h: str) -> FileMetadataResponse:
            return FileMetadataResponse(
                external_id="f",
                name="f.txt",
                id=1,
                created_time=0,
                last_updated_time=0,
                uploaded=True,
                metadata={FileMetadataIO._MetadataKey.FILECONTENT_HASH: h},
            )

        mock_client.filemetadata.retrieve.return_value = [_response(cdf_hash)]
        mock_client.filemetadata.update.return_value = [_response(local_hash)]
        upload_resp = MagicMock(filepath=MagicMock(spec=Path), upload_url="https://upload.example.com", mime_type=None)
        mock_client.filemetadata.get_upload_url.return_value = [upload_resp]

        item = FileMetadataRequest(
            external_id="f",
            name="f.txt",
            metadata={FileMetadataIO._MetadataKey.FILECONTENT_HASH: local_hash},
            filepath=MagicMock(spec=Path),
        )
        fileio.update([item])

        assert mock_client.filemetadata.upload_file.called == expect_upload


class TestCogniteFileIOUpdate:
    @pytest.mark.parametrize(
        "cdf_source_context, local_source_context, expect_upload",
        [
            pytest.param("cognite-toolkit-hash: old", "cognite-toolkit-hash: new", True, id="hash_changed_reuploads"),
            pytest.param("cognite-toolkit-hash: same", "cognite-toolkit-hash: same", False, id="hash_unchanged_skips"),
        ],
    )
    def test_reupload_based_on_hash(
        self, cdf_source_context: str, local_source_context: str, expect_upload: bool
    ) -> None:
        mock_client = MagicMock()
        fileio = CogniteFileIO(mock_client, support_upload=True)
        # Simulates CDF: create (upsert) overwrites the stored source context, retrieve returns the current state.
        cdf_state: dict[NodeId, str | None] = {NodeId(space="sp", external_id="f"): cdf_source_context}

        def _response(node_id: NodeId) -> CogniteFileResponse:
            return CogniteFileResponse(
                space=node_id.space,
                external_id=node_id.external_id,
                version=1,
                created_time=0,
                last_updated_time=0,
                source_context=cdf_state[node_id],
            )

        def _create(items: list[CogniteFileRequest]) -> list[CogniteFileResponse]:
            for item in items:
                cdf_state[item.as_id()] = item.source_context
            return [_response(item.as_id()) for item in items]

        mock_client.cognite_files.create.side_effect = _create
        mock_client.cognite_files.retrieve.side_effect = lambda ids: [_response(id_) for id_ in ids]
        mock_client.filemetadata.get_upload_url.return_value = [MagicMock(upload_url="https://upload.example.com")]

        item = CogniteFileRequest(
            space="sp",
            external_id="f",
            name="f.txt",
            source_context=local_source_context,
            filepath=MagicMock(spec=Path),
        )
        fileio.update([item])

        assert mock_client.filemetadata.upload_file.called == expect_upload
