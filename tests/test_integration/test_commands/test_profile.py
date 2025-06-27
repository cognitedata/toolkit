from cognite.client.data_classes import Transformation

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands import ProfileAssetCentricCommand, ProfileTransformationCommand
from tests.test_integration.constants import ASSET_TABLE


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
