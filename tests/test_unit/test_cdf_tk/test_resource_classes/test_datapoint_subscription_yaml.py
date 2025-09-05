from collections.abc import Iterable
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.resource_classes.datapoint_subscription import DatapointSubscriptionYAML
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from tests.test_unit.utils import find_resources


def invalid_datapoint_subscription_test_cases() -> Iterable:
    yield pytest.param(
        {"name": "Subscription 1"}, {"Missing required field: 'externalId'"}, id="Missing required field: externalId"
    )
    # Test case: more than 10,000 time_series_ids
    yield pytest.param(
        {"externalId": "sub-overlimit", "timeSeriesIds": [f"ts_{i}" for i in range(10_001)]},
        {"The total number of time_series_ids and instance_ids cannot exceed 10000."},
        id="Too many time_series_ids",
    )
    # Test case: both filter and time_series_ids set
    yield pytest.param(
        {"externalId": "sub-filter-and-tsids", "timeSeriesIds": ["ts_1", "ts_2"], "filter": {"some": "filter"}},
        {"Cannot set both filter and time_series_ids/instance_ids."},
        id="Filter and time_series_ids set",
    )


class TestDatapointSubscription:
    @pytest.mark.parametrize("data", list(find_resources("DatapointSubscription")))
    def test_load_valid_dataset(self, data: dict[str, object]) -> None:
        loaded = DatapointSubscriptionYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data, expected_errors", list(invalid_datapoint_subscription_test_cases()))
    def test_invalid_datapoint_subscription_error_messages(self, data: dict | list, expected_errors: set[str]) -> None:
        warning_list = validate_resource_yaml_pydantic(data, DatapointSubscriptionYAML, Path("some_file.yaml"))
        assert len(warning_list) == 1
        format_warning = warning_list[0]
        assert isinstance(format_warning, ResourceFormatWarning)

        assert set(format_warning.errors) == expected_errors
