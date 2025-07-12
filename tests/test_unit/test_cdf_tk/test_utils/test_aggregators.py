from itertools import product

import pytest

from cognite_toolkit._cdf_tk.utils.aggregators import (
    AssetAggregator,
    EventAggregator,
    FileAggregator,
    MetadataAggregator,
    SequenceAggregator,
    TimeSeriesAggregator,
)


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
        actual_dump = actual_filter.dump() if actual_filter is not None else None
        assert actual_dump == expected_filter, (
            f"Failed with {hierarchy}, {data_set_external_id} -> Got {actual_dump}, expected {expected_filter}"
        )
