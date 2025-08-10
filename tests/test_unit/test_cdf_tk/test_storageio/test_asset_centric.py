import pytest
from cognite.client.data_classes import Asset, AssetList, AssetWriteList

from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.storageio import AssetIO
from cognite_toolkit._cdf_tk.storageio._identifiers import AssetCentricData
from cognite_toolkit._cdf_tk.utils.collection import chunker
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal


@pytest.fixture()
def some_asset_data() -> AssetList:
    """Fixture to provide a sample AssetList for testing."""
    return AssetList(
        [
            Asset._load(
                {
                    "externalId": f"asset_{i}",
                    "name": f"Asset {i}",
                    "parentExternalId": None,
                    "description": f"Description for asset {i}",
                    "root": 123,
                    "source": "test_source",
                    "labels": ["my_label"],
                }
            )
            for i in range(100)
        ]
    )


class TestAssetIO:
    def test_download_upload(self, some_asset_data: AssetList) -> None:
        identifier = AssetCentricData(data_set_id=None, hierarchy=("test_hierarchy",))
        with monkeypatch_toolkit_client() as client:
            client.assets.return_value = chunker(some_asset_data, 10)
            client.assets.aggregate_count.return_value = 100
            io = AssetIO(client)

            assert io.count(identifier) == 100

            source = io.download_iterable(identifier)
            json_chunks: list[list[dict[str, JsonVal]]] = []
            for chunk in source:
                json_chunk = io.data_to_json_chunk(chunk)
                assert isinstance(json_chunk, list)
                assert len(json_chunk) == 10
                for item in json_chunk:
                    assert isinstance(item, dict)
                json_chunks.append(json_chunk)

            data_chunks = (io.json_chunk_to_data(chunk) for chunk in json_chunks)
            for data_chunk in data_chunks:
                io.upload_items(data_chunk, identifier)

            assert client.assets.create.call_count == 10
            uploaded_assets = AssetWriteList([])
            for call in client.assets.create.call_args_list:
                uploaded_assets.extend(call[0][0])

            assert uploaded_assets.dump() == some_asset_data.as_write().dump()
