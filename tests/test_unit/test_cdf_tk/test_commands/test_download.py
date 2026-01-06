from pathlib import Path

from cognite.client.data_classes import TransformationPreviewResult

from cognite_toolkit._cdf_tk.client.cdf_client import PagedResponse
from cognite_toolkit._cdf_tk.client.data_classes.asset import AssetAggregateItem, AssetResponse
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands import DownloadCommand
from cognite_toolkit._cdf_tk.storageio import AssetIO
from cognite_toolkit._cdf_tk.storageio.selectors import DataSetSelector
from cognite_toolkit._cdf_tk.utils.fileio import CSVReader


class TestDownloadCommand:
    def test_download_data_in_table_format(self, tmp_path: Path) -> None:
        cmd = DownloadCommand(silent=True, skip_tracking=True)
        with monkeypatch_toolkit_client() as client:
            dataset = "my/:_data_set"
            client.assets.aggregate_count.return_value = 1
            client.tool.assets.iterate.return_value = PagedResponse(
                items=[
                    AssetResponse(
                        id=123,
                        name="asset_123",
                        metadata={"key": "value"},
                        dataSetId=42,
                        aggregates=AssetAggregateItem(childCount=1, depth=0, path=[]),
                        createdTime=0,
                        lastUpdatedTime=0,
                        rootId=0,
                    )
                ]
            )

            client.lookup.data_sets.id.return_value = 42
            client.lookup.data_sets.external_id.return_value = dataset
            client.transformations.preview.return_value = TransformationPreviewResult(
                None, results=[{"key": "key", "key_count": 1}]
            )

            cmd.download(
                selectors=[DataSetSelector(kind="Assets", data_set_external_id=dataset)],
                io=AssetIO(client=client),
                output_dir=tmp_path,
                verbose=True,
                file_format=".csv",
                compression="none",
                limit=-1,
            )

            csv_files = list(tmp_path.rglob("*.csv"))
            assert len(csv_files) == 1
            content = list(CSVReader(csv_files[0]).read_chunks_unprocessed())
            assert content == [
                {
                    "dataSetExternalId": dataset,
                    "description": "",
                    "externalId": "",
                    "geoLocation": "",
                    "labels": "",
                    "metadata.key": "value",
                    "name": "asset_123",
                    "parentExternalId": "",
                    "source": "",
                    "childCount": "1",
                    "depth": "0",
                    "path": "",
                }
            ]
