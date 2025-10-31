from pathlib import Path

from cognite.client.data_classes import TimeSeries

from cognite_toolkit._cdf_tk.tk_warnings import (
    DataSetMissingWarning,
)
from cognite_toolkit._cdf_tk.validation import validate_data_set_is_set


def test_validate_data_set_is_set():
    warnings = validate_data_set_is_set(
        {"externalId": "myTimeSeries", "name": "My Time Series"}, TimeSeries, Path("timeseries.yaml")
    )

    assert sorted(warnings) == sorted(
        [DataSetMissingWarning(Path("timeseries.yaml"), "myTimeSeries", "externalId", "TimeSeries")]
    )
