from itertools import product
from typing import ClassVar
from unittest.mock import MagicMock, patch

import pytest
from _pytest.mark import ParameterSet

from cognite_toolkit._cdf_tk.utils.aggregators import (
    AssetAggregator,
    EventAggregator,
    FileAggregator,
    MetadataAggregator,
    SequenceAggregator,
    TimeSeriesAggregator,
)
from cognite_toolkit._cdf_tk.utils.cdf import label_count, metadata_key_counts
from tests.test_unit.approval_client import ApprovalToolkitClient
from tests.test_unit.approval_client.client import LookUpAPIMock


class TestAggregators:
    @pytest.mark.parametrize(
        "aggregator_class, hierarchy, data_set_external_id, expected_filter",
        [
            (filter_cls, *args)
            for filter_cls, args in product(
                [AssetAggregator, FileAggregator, TimeSeriesAggregator, SequenceAggregator, EventAggregator],
                [
                    (None, None, None),
                    ("hierarchy", None, {"assetSubtreeIds": [{"externalId": "hierarchy"}]}),
                    ("", None, {"assetSubtreeIds": [{"externalId": ""}]}),
                    (None, "data_set", {"dataSetIds": [{"externalId": "data_set"}]}),
                    (None, "", {"dataSetIds": [{"externalId": ""}]}),
                    (
                        "hierarchy",
                        "data_set",
                        {"assetSubtreeIds": [{"externalId": "hierarchy"}], "dataSetIds": [{"externalId": "data_set"}]},
                    ),
                    (
                        "hierarchy",
                        "",
                        {"assetSubtreeIds": [{"externalId": "hierarchy"}], "dataSetIds": [{"externalId": ""}]},
                    ),
                    (
                        "",
                        "data_set",
                        {"assetSubtreeIds": [{"externalId": ""}], "dataSetIds": [{"externalId": "data_set"}]},
                    ),
                    ("", "", {"assetSubtreeIds": [{"externalId": ""}], "dataSetIds": [{"externalId": ""}]}),
                    ([], [], None),
                    (["hierarchy"], [], {"assetSubtreeIds": [{"externalId": "hierarchy"}]}),
                    ([], ["data_set"], {"dataSetIds": [{"externalId": "data_set"}]}),
                    (
                        ["hierarchy"],
                        ["data_set"],
                        {"assetSubtreeIds": [{"externalId": "hierarchy"}], "dataSetIds": [{"externalId": "data_set"}]},
                    ),
                    (
                        ["hierarchy"],
                        [""],
                        {"assetSubtreeIds": [{"externalId": "hierarchy"}], "dataSetIds": [{"externalId": ""}]},
                    ),
                    ([], ["data_set", ""], {"dataSetIds": [{"externalId": "data_set"}, {"externalId": ""}]}),
                ],
            )
        ],
    )
    def test_create_filter_method(
        self,
        aggregator_class: type[MetadataAggregator],
        hierarchy: str | None,
        data_set_external_id: str | None,
        expected_filter: dict[str, object] | None,
    ) -> None:
        actual_filter = aggregator_class.create_filter(hierarchy, data_set_external_id)
        assert (actual_filter is None and expected_filter is None) or actual_filter.dump() == expected_filter, (
            f"Failed with {hierarchy}, {data_set_external_id} -> {actual_filter.dump()} != {expected_filter}"
        )

    hierarchy_dataset_combinations: ClassVar[list[ParameterSet]] = [
        pytest.param(None, None, None, None, id="No hierarchy and no data sets"),
        pytest.param("hierarchy", None, [LookUpAPIMock.create_id("hierarchy")], None, id="Hierarchy only"),
        pytest.param(None, "data_set", None, [LookUpAPIMock.create_id("data_set")], id="Data set only"),
        pytest.param(
            "hierarchy",
            "data_set",
            [LookUpAPIMock.create_id("hierarchy")],
            [LookUpAPIMock.create_id("data_set")],
            id="Hierarchy and data set",
        ),
        pytest.param(
            ["hierarchy1", "hierarchy2"],
            ["data_set1", "data_set2"],
            sorted([LookUpAPIMock.create_id("hierarchy1"), LookUpAPIMock.create_id("hierarchy2")]),
            sorted([LookUpAPIMock.create_id("data_set1"), LookUpAPIMock.create_id("data_set2")]),
            id="Multiple hierarchies and data sets",
        ),
    ]

    @pytest.mark.parametrize("hierarchy, data_sets, hierarchy_ids, data_set_ids", hierarchy_dataset_combinations)
    def test_metadata_keys_count(
        self,
        hierarchy: str | list[str] | None,
        data_sets: str | list[str] | None,
        hierarchy_ids: list[int] | None,
        data_set_ids: list[int] | None,
        toolkit_client_approval: ApprovalToolkitClient,
    ) -> None:
        aggregator = AssetAggregator(toolkit_client_approval.mock_client)

        metadata_key_counts_mock = MagicMock(spec=metadata_key_counts)
        metadata_key_counts_mock.return_value = [("key1", 10), ("key2", 5), ("key3", 15)]

        with patch(f"{AssetAggregator.__module__}.metadata_key_counts", metadata_key_counts_mock):
            result = aggregator.metadata_key_count(hierarchy, data_sets)

        assert result == 3
        metadata_key_counts_mock.assert_called_once_with(
            toolkit_client_approval.mock_client, "assets", hierarchies=hierarchy_ids, data_sets=data_set_ids
        )

    @pytest.mark.parametrize("hierarchy, data_sets, hierarchy_ids, data_set_ids", hierarchy_dataset_combinations)
    def test_label_count(
        self,
        hierarchy: str | list[str] | None,
        data_sets: str | list[str] | None,
        hierarchy_ids: list[int] | None,
        data_set_ids: list[int] | None,
        toolkit_client_approval: ApprovalToolkitClient,
    ) -> None:
        aggregator = AssetAggregator(toolkit_client_approval.mock_client)

        label_count_mock = MagicMock(spec=label_count)
        label_count_mock.return_value = [("label1", 42), ("label2", 10)]

        with patch(f"{AssetAggregator.__module__}.label_count", label_count_mock):
            result = aggregator.label_count(hierarchy, data_sets)

        assert result == 2
        label_count_mock.assert_called_once_with(
            toolkit_client_approval.mock_client, "assets", hierarchies=hierarchy_ids, data_sets=data_set_ids
        )
