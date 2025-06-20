from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands import ProfileCommand


class TestDumpResource:
    def test_profile_assent_centric(self, toolkit_client: ToolkitClient, monkeypatch) -> None:
        results = ProfileCommand().asset_centric(toolkit_client, verbose=False)

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
        total_count = sum(int(item["Count"].replace(",", "")) for item in results)
        assert total_count > 0
        total_metadata_count = 0
        for item in results:
            metadata_count = item.get(ProfileCommand.Columns.MetadataKeyCount, "")
            if "-" in metadata_count or not metadata_count:
                continue
            total_metadata_count += int(metadata_count.replace("-", ""))
        assert total_metadata_count > 0
