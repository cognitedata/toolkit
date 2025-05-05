import pytest

from cognite_toolkit._cdf_tk.resource_classes import TimeSeriesYAML
from tests.test_unit.utils import find_resources


class TestTimeSeriesTK:
    @pytest.mark.parametrize("data", list(find_resources("TimeSeries")))
    def test_load_valid_timeseries(self, data: dict[str, object]) -> None:
        loaded = TimeSeriesYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data
