import itertools
from collections import Counter

import pytest
from cognite.client.data_classes import (
    AssetList,
    AssetWrite,
    AssetWriteList,
    DataSetList,
    DataSetWrite,
    DataSetWriteList,
)

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.utils.cdf import metadata_key_counts


@pytest.fixture(scope="session")
def two_datasets(toolkit_client: ToolkitClient) -> DataSetList:
    datasets = DataSetWriteList(
        [
            DataSetWrite(
                name="Test Dataset 1",
                external_id="toolkit_test_metadata_key_counts_1",
                description="Test Dataset 1",
            ),
            DataSetWrite(
                name="Test Dataset 2",
                external_id="toolkit_test_metadata_key_counts_2",
                description="Test Dataset 2",
            ),
        ]
    )
    existing = toolkit_client.data_sets.retrieve_multiple(
        external_ids=datasets.as_external_ids(), ignore_unknown_ids=True
    )
    if missing := [dataset for dataset in datasets if dataset.external_id not in set(existing.as_external_ids())]:
        created = toolkit_client.data_sets.create(missing)
        existing.extend(created)
    return existing


@pytest.fixture(scope="session")
def two_hierarchies(toolkit_client: ToolkitClient, two_datasets: DataSetList) -> tuple[AssetList, AssetList]:
    hierarchies = [
        AssetWriteList(
            [
                AssetWrite(
                    name=f"Root Asset {i}",
                    external_id=f"toolkit_test_metadata_key_counts_root_asset_{i}",
                    metadata={f"metadata_key_{i}": "does-not-matter"},
                    data_set_id=two_datasets[i].id,
                ),
                AssetWrite(
                    name=f"Child Asset {i}",
                    parent_external_id=f"toolkit_test_metadata_key_counts_root_asset_{i}",
                    data_set_id=two_datasets[i].id,
                    external_id=f"toolkit_test_metadata_key_counts_child_asset_{i}",
                    metadata={
                        f"metadata_key_{i}": "does not matter either",
                        f"metadata_key_extra_{i}": "does not matter either",
                    },
                ),
                AssetWrite(
                    name=f"Child Asset extra {i}",
                    parent_external_id=f"toolkit_test_metadata_key_counts_root_asset_{i}",
                    external_id=f"toolkit_test_metadata_key_counts_child_asset_extra_{i}",
                    metadata={
                        f"extra_key{i}": "does not matter either",
                        f"another_extra_key{i}": "does not matter either",
                    },
                ),
            ]
        )
        for i in range(2)
    ]
    retrieved_hierarchies: list[AssetList] = []
    for hierarchy in hierarchies:
        existing = toolkit_client.assets.retrieve_multiple(
            external_ids=hierarchy.as_external_ids(), ignore_unknown_ids=True
        )
        if missing := [asset for asset in hierarchy if asset.external_id not in set(existing.as_external_ids())]:
            created = toolkit_client.assets.create(missing)
            existing.extend(created)
        retrieved_hierarchies.append(existing)
    return retrieved_hierarchies[0], retrieved_hierarchies[1]


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

    @pytest.mark.parametrize(
        "hierarchies, datasets",
        (
            (hierarchies, datasets)
            for hierarchies, datasets in itertools.product(
                [
                    None,
                    ["toolkit_test_metadata_key_counts_root_asset_0"],
                    ["toolkit_test_metadata_key_counts_root_asset_0", "toolkit_test_metadata_key_counts_root_asset_1"],
                ],
                [
                    None,
                    ["toolkit_test_metadata_key_counts_1"],
                    ["toolkit_test_metadata_key_counts_1", "toolkit_test_metadata_key_counts_2"],
                ],
            )
            if not (not hierarchies and not datasets)
        ),
        ids=(
            f"{hierarchy_str} and {dataset_str}"
            for hierarchy_str, dataset_str in itertools.product(
                ["no hierarchies", "one hierarchy", "two hierarchies"],
                ["no datasets", "one dataset", "two datasets"],
                # We filter out this case as it will return all assets in the project as it has no filtering.
            )
            if not (hierarchy_str == "no hierarchies" and dataset_str == "no datasets")
        ),
    )
    def test_metadata_key_counts_filtering_datasets_hierarchies(
        self,
        hierarchies: list[str] | None,
        datasets: list[str] | None,
        toolkit_client: ToolkitClient,
        two_hierarchies: tuple[AssetList, AssetList],
        two_datasets: DataSetList,
    ) -> None:
        hierarchy_external_to_internal = {assets[0].external_id: assets[0].id for assets in two_hierarchies}
        datasets_external_to_internal = {dataset.external_id: dataset.id for dataset in two_datasets}
        hierarchy_ids = (
            [hierarchy_external_to_internal[external_id] for external_id in hierarchies] if hierarchies else None
        )
        dataset_ids = [datasets_external_to_internal[external_id] for external_id in datasets] if datasets else None
        metadata_keys = metadata_key_counts(toolkit_client, "assets", hierarchies=hierarchy_ids, data_sets=dataset_ids)

        expected_keys = Counter()
        for hierarchy in two_hierarchies:
            for asset in hierarchy:
                if (hierarchy_ids is None or asset.root_id in hierarchy_ids) and (
                    dataset_ids is None or asset.data_set_id in dataset_ids
                ):
                    expected_keys.update(asset.metadata.keys())

        assert {key: count for key, count in metadata_keys} == dict(expected_keys.items())
