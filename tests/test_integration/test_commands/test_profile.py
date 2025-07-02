from cognite.client.data_classes import Asset, Transformation

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands import (
    ProfileAssetCentricCommand,
    ProfileAssetCommand,
    ProfileTransformationCommand,
)
from tests.test_integration.constants import (
    ASSET_COUNT,
    ASSET_DATASET,
    ASSET_TABLE,
    EVENT_COUNT,
    EVENT_DATASET,
    EVENT_TABLE,
    FILE_COUNT,
    FILE_DATASET,
    FILE_TABLE,
    SEQUENCE_COUNT,
    SEQUENCE_DATASET,
    SEQUENCE_TABLE,
    TIMESERIES_COUNT,
    TIMESERIES_DATASET,
    TIMESERIES_TABLE,
)


class TestProfileAssetCommand:
    def test_profile_asset_hierarchy(
        self,
        toolkit_client: ToolkitClient,
        aggregator_root_asset: Asset,
        aggregator_raw_db: str,
        aggregator_assets: Transformation,
        aggregator_events: Transformation,
        aggregator_files: Transformation,
        aggregator_time_series: Transformation,
        aggregator_sequences: Transformation,
    ) -> None:
        results = ProfileAssetCommand().assets(toolkit_client, aggregator_root_asset.external_id)
        columns = ProfileAssetCommand.Columns
        assert results == [
            {
                columns.Resource: resource,
                columns.Count: count,
                columns.DataSets: dataset,
                columns.DataSetCount: count,
                columns.Transformations: f"{transformation.name} ({transformation.external_id})",
                columns.RawTable: f"{aggregator_raw_db}.{raw_table}",
                columns.RowCount: row_count,
                columns.ColumnCount: column_count,
            }
            for resource, count, dataset, transformation, raw_table, row_count, column_count in [
                (
                    "Assets",
                    ASSET_COUNT,
                    ASSET_DATASET,
                    aggregator_assets,
                    ASSET_TABLE,
                    ASSET_COUNT - 1,
                    4,
                ),  # -1 root asset is not in the table
                (
                    "Events",
                    EVENT_COUNT,
                    EVENT_DATASET,
                    aggregator_events,
                    EVENT_TABLE,
                    EVENT_COUNT,
                    5,
                ),
                (
                    "Files",
                    FILE_COUNT,
                    FILE_DATASET,
                    aggregator_files,
                    FILE_TABLE,
                    FILE_COUNT,
                    4,
                ),
                (
                    "TimeSeries",
                    TIMESERIES_COUNT,
                    TIMESERIES_DATASET,
                    aggregator_time_series,
                    TIMESERIES_TABLE,
                    TIMESERIES_COUNT,
                    5,
                ),
                (
                    "Sequences",
                    SEQUENCE_COUNT,
                    SEQUENCE_DATASET,
                    aggregator_sequences,
                    SEQUENCE_TABLE,
                    SEQUENCE_COUNT,
                    4,
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


class TestProfileTransformationCommand:
    def test_profile_transformation(
        self, toolkit_client: ToolkitClient, aggregator_assets: Transformation, aggregator_raw_db: str
    ) -> None:
        results = ProfileTransformationCommand().transformation(toolkit_client, "assets")
        columns = ProfileTransformationCommand.Columns
        search_rows = [row for row in results if row[columns.Transformation] == aggregator_assets.name]
        assert len(search_rows) == 1, "Expected exactly one row for the transformation"
        actual_row = search_rows[0]
        assert actual_row == {
            columns.Transformation: aggregator_assets.name,
            columns.Source: f"{aggregator_raw_db}.{ASSET_TABLE}",
            columns.DestinationColumns: "name, externalId, dataSetId, parentExternalId",
            columns.Destination: "assets",
            columns.ConflictMode: aggregator_assets.conflict_mode,
            columns.IsPaused: "No schedule",
        }
