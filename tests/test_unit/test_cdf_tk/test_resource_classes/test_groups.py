import pytest

from cognite_toolkit._cdf_tk.resource_classes import GroupYAML
from tests.test_unit.utils import find_resources


class TestTimeSeriesTK:
    @pytest.mark.parametrize("data", list(find_resources("Group")))
    def test_load_valid_timeseries(self, data: dict[str, object]) -> None:
        loaded = GroupYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data
