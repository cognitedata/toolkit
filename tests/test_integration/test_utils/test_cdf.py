import itertools
import time
from collections import Counter, defaultdict
from collections.abc import Iterable
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from cognite.client.data_classes import (
    AssetList,
    AssetWrite,
    AssetWriteList,
    DataSetList,
    DataSetWrite,
    DataSetWriteList,
    LabelDefinitionList,
    LabelDefinitionWrite,
    RelationshipList,
    RelationshipWrite,
    RelationshipWriteList,
    RowWriteList,
    TransformationPreviewResult,
)
from cognite.client.data_classes.labels import LabelDefinitionWriteList

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.raw import RawTable
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.constants import MAX_RUN_QUERY_FREQUENCY_MIN
from cognite_toolkit._cdf_tk.exceptions import ToolkitThrottledError
from cognite_toolkit._cdf_tk.utils.cdf import (
    ThrottlerState,
    label_aggregate_count,
    label_count,
    metadata_key_counts,
    raw_row_count,
    relationship_aggregate_count,
)


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
def two_labels(toolkit_client: ToolkitClient, two_datasets: DataSetList) -> LabelDefinitionList:
    data_set_id = two_datasets[0].id  # Using the first dataset for labels
    labels = LabelDefinitionWriteList(
        [
            LabelDefinitionWrite(
                external_id="toolkit_test_label_aggregate_count_1",
                name="Test Label 1",
                description="This is a test label 1",
                data_set_id=data_set_id,
            ),
            LabelDefinitionWrite(
                external_id="toolkit_test_label_aggregate_count_2",
                name="Test Label 2",
                description="This is a test label 2",
                data_set_id=data_set_id,
            ),
        ]
    )
    existing = toolkit_client.labels.retrieve(external_id=labels.as_external_ids(), ignore_unknown_ids=True)
    if missing := [label for label in labels if label.external_id not in set(existing.as_external_ids())]:
        created = toolkit_client.labels.create(missing)
        existing.extend(created)
    return existing


@pytest.fixture(scope="session")
def two_hierarchies(
    toolkit_client: ToolkitClient, two_datasets: DataSetList, two_labels: LabelDefinitionList
) -> tuple[AssetList, AssetList]:
    all_labels = [label.external_id for label in two_labels]
    single_label = [two_labels[0].external_id]
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
                    labels=all_labels if i == 0 else single_label,
                ),
                AssetWrite(
                    name=f"Child Asset extra {i}",
                    parent_external_id=f"toolkit_test_metadata_key_counts_root_asset_{i}",
                    external_id=f"toolkit_test_metadata_key_counts_child_asset_extra_{i}",
                    metadata={
                        f"extra_key{i}": "does not matter either",
                        f"another_extra_key{i}": "does not matter either",
                    },
                    labels=single_label,
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


@pytest.fixture(scope="session")
def asset_relationships(
    toolkit_client: ToolkitClient, two_hierarchies: tuple[AssetList, AssetList], two_datasets: DataSetList
) -> RelationshipList:
    dataset_id = two_datasets[0].id  # Using the first dataset for relationships
    hierarchy, _ = two_hierarchies  # Using the first hierarchy for relationships
    children_by_parent: dict[str, list[str]] = defaultdict(list)
    for asset in hierarchy:
        if asset.parent_external_id and asset.external_id:
            children_by_parent[asset.parent_external_id].append(asset.external_id)

    relationships = RelationshipWriteList(
        [
            RelationshipWrite(
                external_id=f"toolkit_test_relationship_count_{parent}_{child}",
                source_type="asset",
                source_external_id=parent,
                target_type="asset",
                target_external_id=child,
                data_set_id=dataset_id,
            )
            for parent, children in children_by_parent.items()
            for child in children
        ]
    )
    existing = toolkit_client.relationships.retrieve_multiple(
        external_ids=relationships.as_external_ids(), ignore_unknown_ids=True
    )
    if missing := [rel for rel in relationships if rel.external_id not in set(existing.as_external_ids())]:
        created = toolkit_client.relationships.create(missing)
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


class TestRelationshipAggregateCount:
    @pytest.mark.usefixtures("asset_relationships")
    def test_relationship_aggregate_count(self, toolkit_client: ToolkitClient) -> None:
        results = relationship_aggregate_count(toolkit_client)

        # We do not know how many relationship exists in the project, so we only check
        # that there is at least one relationship.
        total = sum(item.count for item in results)
        assert total > 0

    def test_relationship_aggregate_count_with_filtering(
        self, toolkit_client: ToolkitClient, asset_relationships: RelationshipList
    ) -> None:
        assert len(asset_relationships) > 0, "There should be some relationships to test with."
        data_set_id = asset_relationships[0].data_set_id
        assert data_set_id is not None, "The relationships should have a data set ID."
        results = relationship_aggregate_count(toolkit_client, [data_set_id])

        assert len(results) == 1
        item = results[0]
        assert item.source_type == "asset"
        assert item.target_type == "asset"
        assert item.count == len(asset_relationships)


class TestLabelAggregateCount:
    @pytest.mark.usefixtures("two_labels")
    def test_label_aggregate_count(self, toolkit_client: ToolkitClient) -> None:
        total = label_aggregate_count(toolkit_client)

        # We do not know how many labels exists in the project, so we only check
        # that there is at least one label.
        assert total > 0

    def test_label_aggregate_count_with_filtering(
        self, toolkit_client: ToolkitClient, two_labels: LabelDefinitionList
    ) -> None:
        assert len(two_labels) > 0, "There should be some labels to test with."
        data_set_id = two_labels[0].data_set_id
        assert data_set_id is not None, "The labels should have a data set ID."
        count = label_aggregate_count(toolkit_client, [data_set_id])

        assert count == len(two_labels)


class TestLabelCount:
    def test_label_count(self, toolkit_client: ToolkitClient, two_labels: LabelDefinitionList) -> None:
        counts = label_count(toolkit_client, "assets")

        ill_formed = [
            (label, count)
            for label, count in counts
            if not isinstance(label, str) or not isinstance(count, int) or count < 0
        ]
        assert len(counts) > 0, "There should be some label counts."
        assert len(ill_formed) == 0, f"Ill-formed label counts: {ill_formed}"

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
    def test_label_count_filtering_datasets_hierarchies(
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
        label_counts = label_count(toolkit_client, "assets", hierarchies=hierarchy_ids, data_sets=dataset_ids)

        expected_keys = Counter()
        for hierarchy in two_hierarchies:
            for asset in hierarchy:
                if (hierarchy_ids is None or asset.root_id in hierarchy_ids) and (
                    dataset_ids is None or asset.data_set_id in dataset_ids
                ):
                    expected_keys.update([label.external_id for label in asset.labels or []])

        assert {key: count for key, count in label_counts} == dict(expected_keys.items())


@pytest.fixture()
def mocked_tempfile(tmp_path: Path) -> Iterable[Path]:
    """Mock tempfile.gettempdir to return a specific temporary directory."""
    fake_tempdir = MagicMock()
    fake_tempdir.return_value = tmp_path
    with patch(f"{raw_row_count.__module__}.tempfile.gettempdir", fake_tempdir):
        yield tmp_path


class TestRawTableRowCount:
    @pytest.mark.usefixtures("disable_throttler")
    def test_raw_table_row_count(
        self, toolkit_client: ToolkitClient, populated_raw_table: RawTable, raw_data: RowWriteList
    ) -> None:
        count = raw_row_count(toolkit_client, populated_raw_table, max_count=len(raw_data) - 1)

        assert count == len(raw_data) - 1

    def test_raw_table_row_count_raises_throttle_error(
        self, populated_raw_table: RawTable, raw_data: RowWriteList, mocked_tempfile: Path
    ) -> None:
        sleep_time = 0.1
        project = "test_raw_table_row_count_raises_throttle_error_project"
        client = MagicMock(spec=ToolkitClient)
        client.config.project = project
        filepath = ThrottlerState._filepath(project)
        filepath.write_text(str(time.time()), encoding="utf-8")
        time.sleep(sleep_time)

        with pytest.raises(ToolkitThrottledError) as exc_info:
            raw_row_count(client, populated_raw_table)

        assert f"Row count is limited to once every {MAX_RUN_QUERY_FREQUENCY_MIN} minutes. Please wait" in str(
            exc_info.value
        )
        wait_time = exc_info.value.wait_time_seconds
        assert wait_time < (MAX_RUN_QUERY_FREQUENCY_MIN * 60) - sleep_time

    def test_throttler_state_corrupt_file(self, toolkit_client: ToolkitClient, mocked_tempfile: Path) -> None:
        project = "test_throttler_state_corrupt_file_project"
        filepath = ThrottlerState._filepath(project)
        filepath.write_text("corrupt data", encoding="latin-1")

        ThrottlerState.get(project).throttle()
        new_timestamp = float(filepath.read_text(encoding="utf-8"))
        assert new_timestamp > 0, "The timestamp should be reset to a valid value after corruption."

    def test_throttler_state_updated(
        self, populated_raw_table: RawTable, raw_data: RowWriteList, mocked_tempfile: Path
    ) -> None:
        project = "test_throttler_state_updated"
        filepath = ThrottlerState._filepath(project)
        filepath.write_text("0", encoding="utf-8")
        before_last_call_epoch = 0.0
        with monkeypatch_toolkit_client() as client:
            client.config.project = project
            client.transformations.preview.return_value = TransformationPreviewResult(
                None, results=[{"row_count": len(raw_data)}]
            )
            raw_row_count(client, populated_raw_table)

        after_last_call_epoch = float(filepath.read_text(encoding="utf-8"))
        assert after_last_call_epoch > before_last_call_epoch, (
            "The last call epoch should be updated after a successful row count."
        )
