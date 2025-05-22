from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.utils.cdf import metadata_key_counts


class TestMetadataKeyCounts:
    def test_metadata_key_counts(self, toolkit_client: ToolkitClient) -> None:
        metadata_keys = metadata_key_counts(toolkit_client, "assets")
        assert len(metadata_keys) > 0
        ill_formed = [
            key
            for key, count in metadata_keys.items()
            if not isinstance(key, str) or not isinstance(count, int) or count < 0
        ]

        assert len(ill_formed) == 0, f"Ill-formed metadata keys: {ill_formed}"
