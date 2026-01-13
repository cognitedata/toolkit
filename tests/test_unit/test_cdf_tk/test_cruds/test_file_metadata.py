from collections.abc import Iterable
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from _pytest.monkeypatch import MonkeyPatch

from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataRequest, FileMetadataResponse
from cognite_toolkit._cdf_tk.cruds import FileMetadataCRUD
from tests.test_unit.approval_client import ApprovalToolkitClient
from tests.test_unit.approval_client.client import LookUpAPIMock


def file_metadata_config_cases() -> Iterable[tuple]:
    yield pytest.param(
        """externalId: sharepointABC
name: A file.txt
dataSetExternalId: ds_files
source: sharepointABC
""",
        ["1.A file.txt"],
        [
            FileMetadataRequest(
                external_id="sharepointABC",
                source="sharepointABC",
                name="A file.txt",
                data_set_id=LookUpAPIMock.create_id("ds_files"),
            )
        ],
        id="Single file as mapping",
    )
    yield pytest.param(
        """- externalId: sharepointABC
  name: A file.txt
  dataSetExternalId: ds_files
  source: sharepointABC
- externalId: sharepointABC2
  name: Another file.txt
  dataSetExternalId: ds_files
  source: sharepointABC
""",
        ["1.A file.txt", "1.Another file.txt"],
        [
            FileMetadataRequest(
                external_id="sharepointABC",
                source="sharepointABC",
                name="A file.txt",
                data_set_id=LookUpAPIMock.create_id("ds_files"),
            ),
            FileMetadataRequest(
                external_id="sharepointABC2",
                source="sharepointABC",
                name="Another file.txt",
                data_set_id=LookUpAPIMock.create_id("ds_files"),
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
        toolkit_client_approval: ApprovalToolkitClient,
        monkeypatch: MonkeyPatch,
    ) -> None:
        loader = FileMetadataCRUD(toolkit_client_approval.mock_client, None)
        filepath = MagicMock(spec=Path)
        filepath.read_text.return_value = yaml_content
        filepath.parent.glob.return_value = [Path(f) for f in files]
        raw_list = loader.load_resource_file(filepath, {})
        resources = [loader.load_resource(item, is_dry_run=False) for item in raw_list]

        assert [resource.dump() for resource in resources] == [item.dump() for item in expected]

    @staticmethod
    def always_existing_path(*_) -> Path:
        path = MagicMock(spec=Path)
        path.exists.return_value = True
        return path

    def test_dump_file_metadata_without_dataset(
        self, monkeypatch: MonkeyPatch, toolkit_client_approval: ApprovalToolkitClient
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
        loader = FileMetadataCRUD.create_loader(toolkit_client_approval.mock_client)

        dumped = loader.dump_resource(metadata)

        assert dumped == {
            "externalId": "my_file",
            "name": "my_file.txt",
            "mimeType": "text/plain",
        }
