from cognite.client.data_classes import Asset

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands import ProfileAssetCentricCommand, ProfileAssetCommand
from tests.test_integration.constants import (
    ASSET_COUNT,
    ASSET_DATASET,
    ASSET_TRANSFORMATION,
    EVENT_COUNT,
    EVENT_DATASET,
    EVENT_TRANSFORMATION,
    FILE_COUNT,
    FILE_DATASET,
    FILE_TRANSFORMATION,
    SEQUENCE_COUNT,
    SEQUENCE_DATASET,
    SEQUENCE_TRANSFORMATION,
    TIMESERIES_COUNT,
    TIMESERIES_DATASET,
    TIMESERIES_TRANSFORMATION,
)


class TestProfileAssetCommand:
    def test_profile_asset_hierarchy(
        self, toolkit_client: ToolkitClient, aggregator_root_asset: Asset, aggregator_raw_db: str
    ) -> None:
        results = ProfileAssetCommand().assets(toolkit_client, aggregator_root_asset.external_id)
        columns = ProfileAssetCommand.Columns
        assert results == [
            {
                columns.Resource: resource,
                columns.Count: count,
                columns.DataSets: dataset,
                columns.DataSetCount: count,
                columns.Transformations: transformation,
                columns.RawTable: f"{aggregator_raw_db}.{raw_table}",
                columns.RowCount: row_count,
                columns.ColumnCount: column_count,
            }
            for resource, count, dataset, transformation, raw_table, row_count, column_count in [
                (
                    "Assets",
                    ASSET_COUNT,
                    ASSET_DATASET,
                    ASSET_TRANSFORMATION,
                    "toolkit_aggregators_test_asset_transformation",
                    ASSET_COUNT - 1,
                    4,
                ),  # -1 root asset is not in the table
                (
                    "Events",
                    EVENT_COUNT,
                    EVENT_DATASET,
                    EVENT_TRANSFORMATION,
                    "toolkit_aggregators_test_event_transformation",
                    EVENT_COUNT,
                    2,
                ),
                (
                    "Files",
                    FILE_COUNT,
                    FILE_DATASET,
                    FILE_TRANSFORMATION,
                    "toolkit_aggregators_test_file_transformation",
                    FILE_COUNT,
                    2,
                ),
                (
                    "TimeSeries",
                    TIMESERIES_COUNT,
                    TIMESERIES_DATASET,
                    TIMESERIES_TRANSFORMATION,
                    "toolkit_aggregators_test_timeseries_transformation",
                    TIMESERIES_COUNT,
                    2,
                ),
                (
                    "Sequences",
                    SEQUENCE_COUNT,
                    SEQUENCE_DATASET,
                    SEQUENCE_TRANSFORMATION,
                    "toolkit_aggregators_test_sequence_transformation",
                    SEQUENCE_COUNT,
                    2,
                ),
            ]
        ]


class TestDumpResource:
    def test_profile_assent_centric(self, toolkit_client: ToolkitClient, monkeypatch) -> None:
        results = ProfileAssetCentricCommand().asset_centric(toolkit_client, verbose=False)

        assert len(results) == 7
        assert {item["Resource"] for item in results} == {
            "Assets",
            "Events",
            "Files",
            "TimeSeries",
            "Sequences",
            "Relationships",
            "Labels",
        }
        total_count = sum(item["Count"] for item in results)
        assert total_count > 0
        total_metadata_count = 0
        for item in results:
            metadata_count = item.get(ProfileAssetCentricCommand.Columns.MetadataKeyCount)
            if not metadata_count:
                continue
            total_metadata_count += metadata_count
        assert total_metadata_count > 0
