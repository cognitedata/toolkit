from pathlib import Path

import pytest
from cognite.client.data_classes import (
    Asset,
    AssetList,
    AssetWriteList,
    CountAggregate,
    DataSet,
    DataSetList,
    FileMetadataList,
    FileMetadataWriteList,
    LabelDefinition,
    LabelDefinitionList,
)

from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands import DownloadCommand, UploadCommand
from cognite_toolkit._cdf_tk.storageio import AssetIO, FileMetadataIO
from cognite_toolkit._cdf_tk.storageio._selectors import AssetSubtreeSelector, DataSetSelector
from cognite_toolkit._cdf_tk.utils.collection import chunker
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal


@pytest.fixture()
def some_asset_data() -> AssetList:
    """Fixture to provide a sample AssetList for testing."""
    return AssetList(
        [
            Asset(
                external_id=f"asset_{i}",
                name=f"Asset {i}",
                description=f"Description for asset {i}",
                root_id=123,
                source="test_source",
                labels=["my_label"],
                data_set_id=1234,
            )
            for i in range(100)
        ]
    )


class TestAssetIO:
    def test_download_upload(self, some_asset_data: AssetList) -> None:
        selector = AssetSubtreeSelector(hierarchy="test_hierarchy")
        with monkeypatch_toolkit_client() as client:
            client.assets.return_value = chunker(some_asset_data, 10)
            client.assets.aggregate_count.return_value = 100
            client.lookup.data_sets.external_id.return_value = "test_data_set"
            client.lookup.data_sets.id.return_value = 1234

            io = AssetIO(client)

            assert io.count(selector) == 100

            source = io.stream_data(selector)
            json_chunks: list[list[dict[str, JsonVal]]] = []
            for chunk in source:
                json_chunk = io.data_to_json_chunk(chunk)
                assert isinstance(json_chunk, list)
                assert len(json_chunk) == 10
                for item in json_chunk:
                    assert isinstance(item, dict)
                    assert "dataSetExternalId" in item
                    assert item["dataSetExternalId"] == "test_data_set"
                json_chunks.append(json_chunk)

            data_chunks = (io.json_chunk_to_data(chunk) for chunk in json_chunks)
            for data_chunk in data_chunks:
                io.upload_items(data_chunk, selector)

            assert client.assets.upsert.call_count == 10
            uploaded_assets = AssetWriteList([])
            for call in client.assets.upsert.call_args_list:
                uploaded_assets.extend(call[0][0])

            assert uploaded_assets.dump() == some_asset_data.as_write().dump()

    def test_download_upload_command(self, some_asset_data: AssetList, tmp_path: Path) -> None:
        selector = AssetSubtreeSelector(hierarchy="test_hierarchy")
        with monkeypatch_toolkit_client() as client:
            client.assets.return_value = [some_asset_data]
            client.assets.aggregate_count.return_value = 100
            client.lookup.data_sets.external_id.return_value = "test_data_set"
            client.lookup.data_sets.id.return_value = 1234
            client.lookup.assets.external_id.return_value = "test_hierarchy"
            client.lookup.assets.id.return_value = 123
            client.data_sets.retrieve_multiple.return_value = DataSetList(
                [DataSet(id=1234, external_id="test_data_set")]
            )
            client.labels.retrieve.return_value = LabelDefinitionList(
                [LabelDefinition(external_id="my_label", name="my_label")]
            )

            io = AssetIO(client)

            download_command = DownloadCommand(silent=True, skip_tracking=True)
            upload_command = UploadCommand(silent=True, skip_tracking=True)

            download_command.download(
                selectors=[selector],
                io=io,
                output_dir=tmp_path,
                verbose=False,
                file_format=".csv",
                compression="none",
                limit=100,
            )

            upload_command.upload(
                io=io,
                input_dir=tmp_path / io.FOLDER_NAME,
                ensure_configurations=True,
                dry_run=False,
                verbose=False,
            )

            assert client.assets.upsert.call_count == 1
            args, _ = client.assets.upsert.call_args
            assert len(args) == 1
            uploaded_assets = args[0]
            assert isinstance(uploaded_assets, AssetWriteList)
            assert [asset.dump() for asset in uploaded_assets] == [asset.as_write().dump() for asset in some_asset_data]


@pytest.fixture()
def some_filemetadata_data() -> FileMetadataList:
    """Fixture to provide a sample FileMetadataList for testing."""
    from cognite.client.data_classes import FileMetadata

    return FileMetadataList(
        [
            FileMetadata(
                external_id=f"file_{i}",
                name=f"File {i}",
                directory="/test/dir",
                mime_type="text/plain",
                data_set_id=1234,
                asset_ids=[123],
                source="test_source",
            )
            for i in range(50)
        ]
    )


class TestFileMetadataIO:
    def test_download_upload(self, some_filemetadata_data: FileMetadataList) -> None:
        selector = DataSetSelector(data_set_external_id="DataSetSelector")
        with monkeypatch_toolkit_client() as client:
            client.files.return_value = chunker(some_filemetadata_data, 10)
            client.files.aggregate.return_value = [CountAggregate(50)]
            client.lookup.data_sets.external_id.return_value = "test_data_set"
            client.lookup.data_sets.id.return_value = 1234
            client.lookup.assets.external_id.return_value = ["test_hierarchy"]
            client.lookup.assets.id.return_value = [123]

            io = FileMetadataIO(client)

            assert io.count(selector) == 50

            source = io.stream_data(selector)
            json_chunks: list[list[dict[str, JsonVal]]] = []
            for chunk in source:
                json_chunk = io.data_to_json_chunk(chunk)
                assert isinstance(json_chunk, list)
                assert len(json_chunk) == 10
                for item in json_chunk:
                    assert isinstance(item, dict)
                    assert "dataSetExternalId" in item
                    assert item["dataSetExternalId"] == "test_data_set"
                json_chunks.append(json_chunk)

            data_chunks = (io.json_chunk_to_data(chunk) for chunk in json_chunks)
            for data_chunk in data_chunks:
                io.upload_items(data_chunk, selector)

            assert client.files.create.call_count == len(some_filemetadata_data)
            uploaded_files = FileMetadataWriteList([])
            for call in client.files.create.call_args_list:
                uploaded_files.append(call[0][0])

            assert uploaded_files.dump() == some_filemetadata_data.as_write().dump()
