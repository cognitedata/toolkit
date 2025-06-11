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
        total_count = sum(item["Count"] for item in results)
        assert total_count > 0
        total_metadata_count = sum(
            item["Metadata Key Count"] for item in results if isinstance(item.get("Metadata Key Count"), int)
        )
        assert total_metadata_count > 0
