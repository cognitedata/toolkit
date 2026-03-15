from pathlib import Path

from cognite_toolkit._cdf_tk.client.resource_classes.timeseries import TimeSeriesResponse
from cognite_toolkit._cdf_tk.tk_warnings import (
    DataSetMissingWarning,
)
from cognite_toolkit._cdf_tk.validation import validate_data_set_is_set


def test_validate_data_set_is_set():
    warnings = validate_data_set_is_set(
        {"externalId": "myTimeSeries", "name": "My Time Series"}, TimeSeriesResponse, Path("timeseries.yaml")
    )

    assert sorted(warnings) == sorted(
        [DataSetMissingWarning(Path("timeseries.yaml"), "myTimeSeries", "externalId", "TimeSeries")]
    )
