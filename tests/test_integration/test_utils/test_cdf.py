from collections import Counter

import pytest
from cognite.client.data_classes import AssetList, AssetWrite, AssetWriteList

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.utils.cdf import metadata_key_counts


@pytest.fixture(scope="session")
def small_hierarchy(toolkit_client: ToolkitClient) -> AssetList:
    assets = AssetWriteList(
        [
            AssetWrite(
                name="Root Asset",
                external_id="toolkit_test_metadata_key_counts_root_asset",
                metadata={"metadata_key_count_test": "does-not-matter"},
            ),
            AssetWrite(
                name="Child Asset",
                parent_external_id="toolkit_test_metadata_key_counts_root_asset",
                external_id="toolkit_test_metadata_key_counts_child_asset",
                metadata={
                    "metadata_key_count_test": "does not matter either",
                    "metadata_key_count_test_2": "does not matter either",
                },
            ),
        ]
    )
    existing = toolkit_client.assets.retrieve_multiple(external_ids=assets.as_external_ids(), ignore_unknown_ids=True)
    if missing := [asset for asset in assets if asset.external_id not in set(existing.as_external_ids())]:
        created = toolkit_client.assets.create(missing)
        existing.extend(created)
    return existing


class TestMetadataKeyCounts:
    def test_metadata_key_counts(self, toolkit_client: ToolkitClient) -> None:
        metadata_keys = metadata_key_counts(toolkit_client, "events")
        assert len(metadata_keys) > 0
        ill_formed = [
            (key, count)
            for key, count in metadata_keys
            if not isinstance(key, str) or not isinstance(count, int) or count < 0
        ]

        assert len(ill_formed) == 0, f"Ill-formed metadata keys: {ill_formed}"

    def test_metadata_key_counts_root_asset(self, toolkit_client: ToolkitClient, small_hierarchy: AssetList) -> None:
        metadata_keys = metadata_key_counts(toolkit_client, "assets", hierarchies=[small_hierarchy[0].root_id])

        expected_keys = Counter()
        for asset in small_hierarchy:
            expected_keys.update(asset.metadata.keys())

        assert {key: count for key, count in metadata_keys} == dict(expected_keys.items())
