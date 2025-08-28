import pytest
from cognite.client.data_classes import (
    Asset,
)
from cognite.client.exceptions import CogniteAPIError

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.utils.aggregators import (
    AssetAggregator,
    AssetCentricAggregator,
    EventAggregator,
    FileAggregator,
    SequenceAggregator,
    TimeSeriesAggregator,
)
from tests.test_integration.constants import (
    ASSET_COUNT,
    ASSET_DATASET,
    ASSET_TRANSFORMATION,
    EVENT_COUNT,
    EVENT_DATASET,
    EVENT_TRANSFORMATION,
    FILE_COUNT,
    FILE_DATASET,
    FILE_TRANSFORMATION,
    SEQUENCE_COUNT,
    SEQUENCE_DATASET,
    SEQUENCE_TRANSFORMATION,
    TIMESERIES_COUNT,
    TIMESERIES_DATASET,
    TIMESERIES_TRANSFORMATION,
)


class TestAggregators:
    aggregators: tuple[tuple[type[AssetCentricAggregator], str, str, int], ...] = (
        (AssetAggregator, ASSET_TRANSFORMATION, ASSET_DATASET, ASSET_COUNT),
        (TimeSeriesAggregator, TIMESERIES_TRANSFORMATION, TIMESERIES_DATASET, TIMESERIES_COUNT),
        (EventAggregator, EVENT_TRANSFORMATION, EVENT_DATASET, EVENT_COUNT),
        (FileAggregator, FILE_TRANSFORMATION, FILE_DATASET, FILE_COUNT),
        (SequenceAggregator, SEQUENCE_TRANSFORMATION, SEQUENCE_DATASET, SEQUENCE_COUNT),
    )

    @pytest.mark.usefixtures(
        "aggregator_assets", "aggregator_events", "aggregator_files", "aggregator_time_series", "aggregator_sequences"
    )
    @pytest.mark.parametrize(
        "aggregator_class, expected_transformation_external_id, expected_dataset_external_id, expected_count",
        aggregators,
    )
    def test_aggregations(
        self,
        toolkit_client: ToolkitClient,
        aggregator_class: type[AssetCentricAggregator],
        expected_transformation_external_id: str,
        expected_dataset_external_id: str,
        expected_count: int,
        aggregator_root_asset: Asset,
    ) -> None:
        root = aggregator_root_asset.external_id
        aggregator = aggregator_class(toolkit_client)

        try:
            actual_count = aggregator.count(root)
            used_data_sets = aggregator.used_data_sets(root)
            transformation_count = aggregator.transformation_count()
            used_transformations = aggregator.used_transformations(used_data_sets)
        except CogniteAPIError as e:
            if e.code == 500 and "Internal server error" in e.message:
                pytest.skip("Skipping test due to intermittent CDF 500 error.")
            raise e

        assert actual_count == expected_count
        assert used_data_sets == [expected_dataset_external_id]
        assert transformation_count >= 1  # We know at least one transformation is writing to the resource type.
        assert len(used_transformations) == 1
        assert used_transformations[0].external_id == expected_transformation_external_id
