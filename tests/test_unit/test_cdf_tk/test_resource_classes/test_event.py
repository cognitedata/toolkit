import pytest

from cognite_toolkit._cdf_tk.resource_classes import EventYAML
from tests.test_unit.utils import find_resources


class TestAssetYAML:
    @pytest.mark.parametrize("data", list(find_resources("Event")))
    def test_load_valid_asset(self, data: dict[str, object]) -> None:
        loaded = EventYAML.model_validate(data)

        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data
